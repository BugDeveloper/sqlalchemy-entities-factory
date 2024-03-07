import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from http import HTTPStatus
from os import getenv
from typing import Dict, Set, Any
from unittest.mock import patch, MagicMock

import jsonref
import yaml
from cds_common.cds_rds_v1 import tables as db_tables
from cds_common.cds_rds_v1.tables import (
    Base,
    Pipeline,
    StageManualDecision,
    Job,
    ResourceStatusEvent,
)
from jsonschema.exceptions import ValidationError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from jsonschema import validate as validate_by_schema

from tests.api_spec_verification.model_factory_generator import factory_generator

logger = logging.getLogger(__name__)


class ApiVerifierException(Exception):
    pass


class DBInitializer:
    session = None

    primary_keys: Dict[str, str] = {}

    @classmethod
    def start(cls):
        cls.engine = create_engine(
            f"postgresql+psycopg2://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}"
            f"@127.0.0.1:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DB')}"
        )
        Base.metadata.create_all(cls.engine)
        cls.session = sessionmaker(bind=cls.engine)()
        cls._init_data()
        cls.primary_keys = cls._load_primary_keys()

    @classmethod
    def _get_overrides(cls):
        with open(
            "./tests/api_spec_verification/json_fields_fixtures/json_fixtures.json", "r"
        ) as f:
            json_overrides = json.load(f)
        overrides = defaultdict(dict, json_overrides)

        overrides["pipeline_configs"] = {
            "type": "customRollout",
            "git_link": "http://git-link.com",
        }

        overrides["pipeline_inputs"] = {"type": "customRollout"}
        overrides["job"] = {"status": "RUNNING"}

        return overrides

    @classmethod
    def _init_data(cls):
        overrides = cls._get_overrides()
        cls.factory_generator = factory_generator(db_tables, overrides)

        cls.pipeline_factory = cls.factory_generator(Pipeline)
        cls.stage_manual_dec_factory = cls.factory_generator(StageManualDecision)
        cls.job_factory = cls.factory_generator(Job)
        cls.status_event_factory = cls.factory_generator(ResourceStatusEvent)

        cls.session.add(cls.pipeline_factory())
        cls.session.add(cls.stage_manual_dec_factory())
        cls.session.add(cls.job_factory())
        cls.session.add(cls.status_event_factory())

        cls.session.commit()

    @classmethod
    def _load_primary_keys(cls):
        table_primary_keys = {}
        for table_name, primary_key in cls.session.execute(
            """
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
        """
        ):
            primary_key_value = cls.session.execute(
                text(
                    f"""
                SELECT {primary_key}
                FROM {table_name}
                LIMIT 1
            """
                )
            ).scalar()
            if primary_key_value is not None:
                table_primary_keys[table_name] = primary_key_value
        return table_primary_keys

    @classmethod
    def stop(cls):
        if not cls.session:
            return
        cls.session.close()


def _load_spec():
    with open("./src/cds_api/docs/openapi.yml", "r") as f:
        spec = yaml.full_load(f)
    return jsonref.loads(json.dumps(spec))


def _event_factory():
    with open("./tests/api_spec_verification/lambda_event_fixture.json", "r") as f:
        event_template = json.load(f)

    def _get_event(endpoint):
        event = event_template.copy()
        event["path"] = endpoint
        event["pathParameters"] = {"proxy": endpoint}
        event["requestContext"]["path"] = endpoint
        return event

    return _get_event


def _update_nullable(schema: Dict) -> Dict:
    if isinstance(schema, dict):
        if schema.get("nullable") is True:
            schema["type"] = [schema["type"], "null"]
            del schema["nullable"]

        for key, value in schema.items():
            schema[key] = _update_nullable(value)
    elif isinstance(schema, list):
        for index, item in enumerate(schema):
            schema[index] = _update_nullable(item)
    return schema


def _update_nullable_to_json_schema_compatible(schema):
    return _update_nullable(schema.copy())


class FakeSSMClient:
    @staticmethod
    def get_parameter(**kwargs: Any) -> dict:
        return {
            "Parameter": {
                "Name": kwargs.get("Name"),
                "Type": "SecureString",
                "Value": "mocked_decrypted_value_from_ssm",
                "Version": 3,
                "LastModifiedDate": datetime.now(),
                "ARN": "some ARN",
                "DataType": "text",
            },
            "ResponseMetadata": {},
        }

    @staticmethod
    def get_parameters_by_path(**kwargs: Any) -> dict:
        return {
            "Parameters": [
                {
                    "Name": kwargs.get("Path"),
                    "Type": "SecureString",
                    "Value": "mocked_decrypted_value_from_ssm",
                    "Version": 3,
                    "LastModifiedDate": datetime.now(),
                    "ARN": "some ARN",
                    "DataType": "text",
                }
            ],
            "ResponseMetadata": {},
        }


@patch("cds_common.injections._get_app_registry")
@patch("db_utils.CdsApiDbConnector")
@patch("blueprints.cmp.db_utils.CmpDbConnector")
@patch("cds_common.injections._get_global_settings")
@patch("settings.global_settings.GlobalSettings")
@patch("lib.ldap.utils.get_ldap_settings")
@patch("aws_class_based_api.controller.base_views.base_view.BaseView.check_auth")
@patch("boto3.client")
def test_verify_api(
    boto3_client,
    check_auth,
    get_ldap,
    global_settings,
    get_global_settings,
    cmp_connector,
    api_connector,
    app_registry,
):
    DBInitializer.start()
    spec = _load_spec()

    global_settings.get_settings.return_value = MagicMock()
    get_global_settings.return_value = global_settings
    app_registry.return_value = MagicMock()
    get_ldap.return_value = None
    check_auth.return_value = None

    api_connector.get_cds_api_session.return_value = DBInitializer.session
    cmp_connector.get_cmp_engine_session.return_value = (None, None)

    boto3_client.return_value = FakeSSMClient()

    from lambda_handler import main as handler_main
    from test_utils import get_view_by_full_path

    class LambdaContext:
        aws_request_id = ""

    schema_errors: Dict[str, str] = {}
    http_errors: Dict[str, int] = {}
    empty_responses: Set[str] = set()
    successful_checks: Set[str] = set()

    views_by_spec_path = get_view_by_full_path()

    event_factory = _event_factory()
    for endpoint, endpoint_data in spec["paths"].items():
        if (
            "/app-registry/" in endpoint
            or "/cds/" not in endpoint
            or "get" not in endpoint_data
            or "200" not in endpoint_data["get"]["responses"]
        ):
            continue

        full_api_endpoint = f"api/v2{endpoint}"

        pattern = r"\{([^}]+)\}"
        primary_keys_in_link = re.findall(pattern, endpoint)

        if primary_keys_in_link:
            view = views_by_spec_path.get(endpoint)
            if not view:
                logger.warning(
                    f"View could not be found or {full_api_endpoint}. Skipping"
                )
                continue
            endpoint_table_name = view.backend.model.__tablename__
            created_pk = DBInitializer.primary_keys.get(endpoint_table_name)
            if not created_pk:
                logger.warning(
                    f"Primary key not found for {full_api_endpoint}. Skipping"
                )
                continue
            full_api_endpoint = full_api_endpoint.replace(
                f"{{{primary_keys_in_link[0]}}}", str(created_pk)
            )

        schema = endpoint_data["get"]["responses"]["200"]["content"][
            "application/vnd.api+json"
        ]["schema"]

        schema = _update_nullable_to_json_schema_compatible(schema)

        response = handler_main(
            event_factory(full_api_endpoint),
            LambdaContext,
        )

        if response["statusCode"] != HTTPStatus.OK:
            http_errors[full_api_endpoint] = response["statusCode"]
            continue

        result = json.loads(response.get("body"))

        if not result["data"]:
            empty_responses.add(endpoint)
            continue

        try:
            validate_by_schema(instance=result, schema=schema)
            successful_checks.add(endpoint)
        except ValidationError as e:
            schema_errors[endpoint] = e.message

    errors = any([http_errors, schema_errors, empty_responses])

    if errors:
        raise ApiVerifierException(
            f"Success endpoints: {len(successful_checks)}\nEmpty responses: {empty_responses}\n"
            f"Server internal errors: {http_errors}\nSpec schema validation errors: {schema_errors}"
        )
    checks_result = "\nGET: ".join(successful_checks)
    print(f"Successfully checked: \n\nGET: {checks_result}")
