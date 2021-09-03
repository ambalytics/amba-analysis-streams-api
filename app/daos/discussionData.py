from typing import List
from sqlalchemy import text
from sqlalchemy.orm import Session  # type: ignore
from sqlalchemy import asc, desc
from event_stream.models.model import DiscussionData


def get_items(session: Session, offset: int = 0, limit: int = 100, sort: str = 'id', order: str = 'asc') -> List[DiscussionData]:
    if order != 'asc':
        order_by = desc(text(sort))
    else:
        order_by = asc(text(sort))
    return session.query(DiscussionData).order_by(order_by).offset(offset).limit(limit).all()


def get_newest(session: Session):
    s = text('SELECT "createdAt" FROM "DiscussionData" ORDER BY "createdAt" DESC LIMIT 1')
    return session.execute(s).fetchall()