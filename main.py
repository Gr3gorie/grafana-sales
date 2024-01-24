from fast_bitrix24 import BitrixAsync
import asyncio
from pydantic import BaseModel
from datetime import datetime
from typing import Union, List
import asyncpg
import os

webhook = os.environ["BITRIX_WEBHOOK"]
b = BitrixAsync(webhook)


class Stage(BaseModel):
    name: str
    status_id: str


async def get_stages():
    raw_stages = await b.get_all('crm.status.list', params={
        'select': ["NAME", "STATUS_ID"],
        'filter': {"ENTITY_ID": "DEAL_STAGE"}
    })

    assert isinstance(raw_stages, list)
    stages = [
        Stage(name=stage["NAME"], status_id=stage["STATUS_ID"])
        for stage in raw_stages
    ]
    return stages


class Source(BaseModel):
    name: str
    status_id: str


async def get_sources():
    raw_sources = await b.get_all('crm.status.list', params={
        'select': ["NAME", "STATUS_ID"],
        'filter': {"ENTITY_ID": "SOURCE"}
    })
    assert isinstance(raw_sources, list)
    sources = [
        Source(name=source["NAME"], status_id=source["STATUS_ID"])
        for source in raw_sources
    ]
    return sources


class User(BaseModel):
    id: int
    name: str
    last_name: str


async def get_users():
    raw_users = await b.get_all('user.get', params={
        'select': ["ID", "NAME", "LAST_NAME"]
    })
    assert isinstance(raw_users, list)
    users = [
        User(id=user["ID"],
             name=user["NAME"],
             last_name=user["LAST_NAME"])
        for user in raw_users
    ]
    return users


class Deal(BaseModel):
    id: int
    name: str
    stage_id: str
    opportunity: float
    begin_date: datetime
    close_date: datetime
    created_by: int
    is_closed: bool
    source_id: Union[str, None]


async def get_deals():
    raw_deals = await b.get_all('crm.deal.list', params={
        'select': ["ID", "TITLE", "STAGE_ID", "OPPORTUNITY",
                   "BEGINDATE", "CLOSEDATE", "CREATED_BY_ID",
                   "CLOSED", "SOURCE_ID"]
    })
    assert isinstance(raw_deals, list)
    deals = [
        Deal(id=deal["ID"], name=deal["TITLE"], stage_id=deal["STAGE_ID"],
             opportunity=deal["OPPORTUNITY"], begin_date=deal["BEGINDATE"],
             close_date=deal["CLOSEDATE"], created_by=deal["CREATED_BY_ID"],
             is_closed=deal["CLOSED"] == "Y", source_id=deal["SOURCE_ID"])
        for deal in raw_deals
    ]
    return deals


async def run_migrations(conn: asyncpg.Connection):
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS stage (
        name VARCHAR(255) NOT NULL,
        status_id VARCHAR(255) PRIMARY KEY
        );
    ''')

    await conn.execute('''
        CREATE TABLE IF NOT EXISTS source (
        name VARCHAR(255) NOT NULL,
        status_id VARCHAR(255) PRIMARY KEY
        );
    ''')

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS person (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL
    );
    ''')

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS deal (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    stage_id VARCHAR(255),
    opportunity FLOAT NOT NULL,
    begin_date TIMESTAMPTZ NOT NULL,
    close_date TIMESTAMPTZ NOT NULL,
    created_by INT REFERENCES person(id),
    is_closed BOOLEAN NOT NULL,
    source_id VARCHAR(255)
    );
    ''')


async def postgres_insert_stages(conn: asyncpg.Connection, stages: List[Stage]):
    for stage in stages:
        await conn.execute('''
        INSERT INTO stage(name, status_id) VALUES($1, $2)
        ON CONFLICT (status_id)
        DO UPDATE
        SET name = $1
        ''', stage.name, stage.status_id)


async def postgres_insert_sources(conn: asyncpg.Connection, sources: List[Source]):
    for source in sources:
        await conn.execute('''
        INSERT INTO source(name, status_id) VALUES($1, $2)
        ON CONFLICT (status_id)
        DO UPDATE
        SET name = $1
        ''', source.name, source.status_id)


async def postgres_insert_users(conn: asyncpg.Connection, users: List[User]):
    for user in users:
        await conn.execute('''
        INSERT INTO person(id, name, last_name) VALUES($1, $2, $3)
        ON CONFLICT (id)
        DO UPDATE
        SET name = $2, last_name = $3
        ''', user.id, user.name, user.last_name)


async def postgres_insert_deals(conn: asyncpg.Connection, deals: List[Deal]):
    for deal in deals:
        await conn.execute('''
        INSERT INTO deal(id, name, stage_id, opportunity, 
        begin_date, close_date, created_by, is_closed, 
        source_id) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (id)
        DO UPDATE
        SET name = $2, stage_id = $3, opportunity = $4, 
        begin_date = $5, close_date = $6, created_by = $7, is_closed = $8, 
        source_id = $9
        ''', deal.id, deal.name, deal.stage_id,
                           deal.opportunity, deal.begin_date,
                           deal.close_date, deal.created_by,
                           deal.is_closed, deal.source_id)


async def main():
    print("Connecting to Postgres...")
    conn = await asyncpg.connect(user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
                                 database=os.environ["POSTGRES_DATABASE"], host=os.environ["POSTGRES_HOST"],
                                 port=os.environ["POSTGRES_PORT"])

    print("Running migrations...")
    await run_migrations(conn)

    print("Getting data from Bitrix")
    stages = await get_stages()
    sources = await get_sources()
    users = await get_users()
    deals = await get_deals()

    print("Inserting Bitrix data to Postgres")
    await postgres_insert_stages(conn, stages)
    await postgres_insert_sources(conn, sources)
    await postgres_insert_users(conn, users)
    await postgres_insert_deals(conn, deals)

    print("Done")


if __name__ == '__main__':
    asyncio.run(main())
