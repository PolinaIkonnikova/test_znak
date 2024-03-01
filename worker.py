import asyncio
import os
import json
import logging
import sys
import asyncpg

from dotenv import load_dotenv
from typing import Dict, Any, List, Tuple


logger = logging.getLogger(__name__)


def _set_log_conf(log):
    """ Настройка логов """
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    log.addHandler(stdout_handler)


async def _update_records_to_db(conn_params, data_tbl, docs_tbl):
    """ Добавляет тестовые данные в таблицы """

    data_records = [(r["level"], r["object"], r["owner"], r["parent"], r["status"]) for r in
                    data_tbl]
    docs_records = [(r["doc_id"], r["recieved_at"], r["document_type"], r["document_data"]) for r in
                    docs_tbl]
    connection = await asyncpg.connect(**conn_params)
    async with connection.transaction():
        await connection.executemany('''
                    INSERT INTO data (level, object, owner, parent, status)
                    VALUES ($1, $2, $3, $4, $5)
                ''', data_records)
        await connection.executemany("""
                    INSERT INTO documents (doc_id, recieved_at, document_type, document_data)
                    VALUES ($1, $2, $3, $4)
        """, docs_records)
    await connection.close()


def get_update_query(fields: List[Tuple[str, Any]]) -> str:
    """ Составляет запрос на обновление """
    # asyncpg ругался на синтаксис с именованными параметрами для вставки значений (:var),
    # поэтому вместо словарей для вставки значений выбрана конструкция List[Tuple[str, Any]]

    changed_field_strings = [f"{item[0]} = ${i + 1}" for i, item in enumerate(fields[:-1])]
    return f"""
    UPDATE data SET {", ".join(changed_field_strings)} WHERE object = ${len(fields)}
    """


async def update_tables(conn_params: Dict) -> bool:
    """ Обновляет данные в таблицах """
    result = True
    query_select_docs = """
            SELECT * FROM documents WHERE document_type='transfer_document'
            and processed_at is Null ORDER BY recieved_at;
            """
    query_select_data = """
            SELECT * FROM data WHERE parent = ANY($1);
            """
    query_insert_time = """
            UPDATE documents SET processed_at = CURRENT_TIMESTAMP WHERE doc_id = $1;
            """
    connection = await asyncpg.connect(**conn_params)
    try:
        async with connection.transaction():
            async for doc in connection.cursor(query_select_docs):
                doc_id = doc["doc_id"]
                doc_data = json.loads(doc["document_data"])
                obj_keys: List = doc_data['objects']
                operation_details: Dict = doc_data['operation_details']

                # Ниже пояснение, почему эта переменная закомментирована
                # changed_objs = []

                entities: List = await connection.fetch(query_select_data, obj_keys)
                for entity in entities:
                    obj_id = entity.get("object")
                    data_for_update_query: List[Tuple[str, Any]] = []
                    for key, val in operation_details.items():

                        # исходя из задания, замена происходит только в случае соответствия
                        # текущего значения entity с значением "old"
                        if entity[key] == val['old']:
                            data_for_update_query.append((key, val['new']))

                    if not data_for_update_query:
                        continue
                    data_for_update_query.append(("object", obj_id))

                    # changed_objs.append(data_for_update_query)
                    # была надежда, что для группы changed_objs возможно использовать
                    # 1 запрос для всех объектов, тогда можно было бы выполнить
                    # connection.executemany,
                    # но из-за проверки entity[key] == val['old'] некоторые поля не подлежат
                    # обновлению, по-этому сгруппировать не удалось

                    update_query = get_update_query(data_for_update_query)
                    await connection.execute(update_query,
                                             *(i[1] for i in data_for_update_query))

                await connection.execute(query_insert_time, doc_id)

    except asyncpg.PostgresError as err:
        logger.error(f"Ошибка при работе с бд: {err}")
        result = False
    except Exception as err:
        logger.error(f"Непредвиденная ошибка: {err}")
        result = False
    finally:
        await connection.close()
    return result


async def main():
    load_dotenv()
    _set_log_conf(logger)

    connection_params = {
        'user': os.getenv("PG_USER"),
        'password': os.getenv("PASSWORD"),
        'database': os.getenv("DATABASE"),
        'host': os.getenv("HOST"),
        'port': os.getenv("PORT"),
    }

    res = await update_tables(connection_params)
    logger.debug(f"Результат обновления - {res}")


if __name__ == '__main__':
    asyncio.run(main())
