from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session  # type: ignore

from app.models.schema import StatValue, DiscussionData, TimeValue
from app.daos.database import SessionLocal, engine
from app.daos.discussionData import (
    get_items,
    get_newest,
)
import event_stream.models.model as models

models.Base.metadata.create_all(bind=engine)
router = APIRouter()


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@router.get("/", response_model=List[DiscussionData])
def read_items(
    offset: int = 0, limit: int = 10, sort: str = 'id', order: str = 'asc', session: Session = Depends(get_session)
):
    # todo filter
    # fields=None, order: str = "id", asc: bool = False, limit: int = 100, offset: int = 0, session: Session = Depends(get_session)
    # if fields is None:
    #     fields = ['id', 'score']
    # items = get_items(session=session, fields=fields, order=order, asc=asc, limit=limit, offset=offset)
    items = get_items(session=session, offset=offset, limit=limit, sort=sort, order=order)
    return items


@router.get("/newest", response_model=StatValue)
def get_newest_item(session: Session = Depends(get_session)):
    item = get_newest(session=session)[0]
    return StatValue(value=item.createdAt.strftime('%Y-%m-%d %H:%M:%S.%f'), name='created_at')
