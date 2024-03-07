import json
from collections import defaultdict

from cds_common.utils import get_env
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import JSONB, JSON
from sqlalchemy.orm import sessionmaker

"""
    The script serves to simplify JSON fields fixtures creation.
    You can edit connection string below to db from which you want JSON fixtures to be taken and run.
    The script will put data near itself to json_fixtures.json file.
    After this just upload it to git.
"""


def main():
    db_url = (
        f"postgresql://{get_env('DB_USER')}:{get_env('DB_PASSWORD')}"
        f"@{get_env('DB_HOST')}:{get_env('DB_PORT')}/{get_env('DB_NAME')}"
    )

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    inspector = inspect(engine)
    schema_name = "public"
    table_names = inspector.get_table_names(schema=schema_name)

    json_fixtures = defaultdict(dict)
    for table_name in table_names:
        columns = inspector.get_columns(table_name, schema=schema_name)
        for column in columns:
            column_type = column["type"]
            if not isinstance(column_type, (JSON, JSONB)):
                continue

            query = text(
                f"SELECT {column['name']} FROM {schema_name}.{table_name} "
                f"WHERE {column['name']} IS NOT NULL "
                f"AND {column['name']}::text != 'null'::text "
                f"AND {column['name']}::text != '{{}}'::text "
                f"AND {column['name']}::text != '[]'::text LIMIT 1"
            )
            result = session.execute(query).scalar()

            if result:
                json_fixtures[table_name][column["name"]] = result
                continue

            query = text(
                f"SELECT {column['name']} FROM {schema_name}.{table_name} "
                f"WHERE {column['name']} IS NOT NULL "
                f"AND {column['name']}::text != 'null'::text"
            )
            result = session.execute(query).scalar()

            if not result:
                continue

            json_fixtures[table_name][column["name"]] = result

    session.close()

    with open("./json_fixtures.json", "w") as f:
        json.dump(json_fixtures, f)


if __name__ == "__main__":
    main()
