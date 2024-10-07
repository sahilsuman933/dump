from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class File(Base):
    """
    Class File(Base):
        SQLAlchemy ORM model representing the 'file' table in the database.
    """
    __tablename__ = 'file'

    id = Column(String, primary_key=True)
    storageKey = Column(String, unique=True)
    url = Column(String)
    pageContentUrl = Column(String)
    title = Column(String)
    docAuthor = Column(String)
    description = Column(String)
    docSource = Column(String)
    chunkSource = Column(String)
    published = Column(DateTime)
    wordCount = Column(Integer)
    tokenCountEstimate = Column(Integer)
    folderId = Column(String)
    createdAt = Column(DateTime)
    updatedAt = Column(DateTime)