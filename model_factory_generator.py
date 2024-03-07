import inspect
import types
from typing import Set, Dict, Any

import factory
from cds_common.cds_rds_v1.tables import Base as BaseModel
from factory import SubFactory, Factory
from factory.fuzzy import FuzzyFloat
from sqlalchemy import (
    String,
    Integer,
    JSON,
    Boolean,
    DateTime,
    Enum,
    Float,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeMeta, relationship as model_relationship


def _generate_many_to_one(model_factory, relation_name):
    def many_to_one(self, create, extracted, **kwargs):
        if not create:
            return
        getattr(self, relation_name).append(model_factory())

    return factory.post_generation(many_to_one)


def factory_generator(
    models_module: types.ModuleType, overrides: Dict[str, Any] = None
):
    overrides = overrides or {}
    class_members = inspect.getmembers(models_module, inspect.isclass)
    tables_to_models = {
        member.__tablename__: member
        for _, member in class_members
        if isinstance(member, DeclarativeMeta) and hasattr(member, "__tablename__")
    }
    data_providers = {
        UUID: lambda _: factory.Faker("uuid4"),
        String: lambda col: factory.Faker("pystr", max_chars=col.type.length or 36)
        if not col.primary_key
        else factory.Faker("uuid4"),
        Integer: lambda _: factory.Faker("random_int"),
        Float: lambda _: FuzzyFloat(0.0),
        Boolean: lambda _: factory.Faker("pybool"),
        DateTime: lambda _: factory.Faker("date_this_month"),
        Enum: lambda col: factory.Faker(
            "random_element", elements=[value for value in col.type.enums]
        ),
        postgresql.ENUM: lambda col: factory.Faker(
            "random_element", elements=[value for value in col.type.enums]
        ),
        JSON: lambda _: {},
        postgresql.JSONB: lambda _: {},
    }

    model_instance_registry: Dict[str, BaseModel] = {}
    factory_registry: Dict[str, Factory] = {}

    class RegistryModelFactory(Factory):
        class Meta:
            abstract = True

        @classmethod
        def _create(cls, model_class, *args, **kwargs):
            instance_key = model_class.__tablename__
            if instance_key in model_instance_registry:
                return model_instance_registry[instance_key]
            instance = super()._create(model_class, *args, **kwargs)
            model_instance_registry[instance_key] = instance
            return instance

    def make_factory(model: DeclarativeMeta, processed_tables: Set[str] = None):
        table_name = model.__table__.fullname

        if table_name in factory_registry:
            return factory_registry[table_name]

        factory_attributes = {}
        processed_tables = set(processed_tables or set())
        processed_tables.add(table_name)

        factory_meta = type("Meta", (), {"model": model})

        relation_cols = {
            next(iter(relation.local_columns)).key
            for relation in model.__mapper__.relationships
        }

        dynamic_relationships = []

        for col in model.__table__.c._all_columns:
            if col.foreign_keys:
                if col.key in relation_cols or col.key[-3:] != "_id":
                    continue
                dynamic_relationships.append(
                    {
                        "foreign_table_name": next(
                            iter(col.foreign_keys)
                        ).column.table.fullname,
                        "relation_name": col.key[:-3],
                    }
                )

            override = (overrides.get(table_name) or {}).get(col.name)  # type: ignore
            if override:
                factory_attributes[col.name] = override
                continue

            factory_attributes[col.name] = data_providers[type(col.type)](col)

        for relationship in list(model.__mapper__.relationships):
            foreign_table_name = relationship.target.fullname

            if foreign_table_name in processed_tables:
                continue

            foreign_model = tables_to_models[foreign_table_name]
            foreign_model_factory = make_factory(foreign_model, processed_tables)

            factory_attributes[relationship.key] = (
                SubFactory(foreign_model_factory)
                if not relationship.uselist
                else _generate_many_to_one(foreign_model_factory, relationship.key)
            )

        for dynamic_relationship in dynamic_relationships:
            foreign_model = tables_to_models[dynamic_relationship["foreign_table_name"]]
            foreign_model_factory = make_factory(foreign_model, processed_tables)
            setattr(
                model,
                dynamic_relationship["relation_name"],
                model_relationship(foreign_model.__name__),
            )
            if hasattr(model, "serialize_rules"):
                model.serialize_rules = model.serialize_rules + (
                    f"-{dynamic_relationship['relation_name']}",
                )

            factory_attributes[dynamic_relationship["relation_name"]] = SubFactory(
                foreign_model_factory
            )

        factory_attributes["Meta"] = factory_meta
        factory_registry[table_name] = type(
            model.__name__, (RegistryModelFactory,), factory_attributes
        )

        return factory_registry[table_name]

    return make_factory
