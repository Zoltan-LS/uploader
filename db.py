# db.py
from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, DateTime, BigInteger, Index, text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/file_storage_db"
)

# PostgreSQL-specific engine configuration
engine = create_engine(
    DATABASE_URL,
    pool_size=10,  # Connection pool size
    max_overflow=20,  # Max connections beyond pool_size
    pool_pre_ping=True,  # Verify connections before using
    pool_recycle=3600,  # Recycle connections after 1 hour
    echo=False  # Set to True for SQL debugging
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class File(Base):
    __tablename__ = "files"
    
    id = Column(String(36), primary_key=True, index=True)  # UUID length
    filename = Column(String(255), nullable=False, index=True)
    size = Column(BigInteger, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    chunks = relationship("FileChunk", back_populates="file", cascade="all, delete-orphan")
    
    # PostgreSQL-specific indexes
    __table_args__ = (
        Index('idx_files_filename_lower', text('LOWER(filename)')),  # Case-insensitive search
        Index('idx_files_created_at', created_at),
        Index('idx_files_size', size),
    )

class Chunk(Base):
    __tablename__ = "chunks"
    
    hash = Column(String(64), primary_key=True)  # SHA256 is 64 chars
    path = Column(String(512), nullable=False)
    ref_count = Column(Integer, default=1, nullable=False, index=True)
    size = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    files = relationship("FileChunk", back_populates="chunk", cascade="all, delete-orphan")
    
    # PostgreSQL-specific indexes
    __table_args__ = (
        Index('idx_chunks_ref_count', ref_count),
        Index('idx_chunks_created_at', created_at),
    )

class FileChunk(Base):
    __tablename__ = "file_chunks"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String(36), ForeignKey("files.id", ondelete="CASCADE"), nullable=False, index=True)
    chunk_hash = Column(String(64), ForeignKey("chunks.hash", ondelete="CASCADE"), nullable=False, index=True)
    order_index = Column(Integer, nullable=False)
    
    # Relationships
    file = relationship("File", back_populates="chunks")
    chunk = relationship("Chunk", back_populates="files")
    
    # Composite index for faster lookups
    __table_args__ = (
        Index('idx_file_chunks_file_order', file_id, order_index),
        Index('idx_file_chunks_chunk_hash', chunk_hash),
        UniqueConstraint('file_id', 'order_index', name='uq_file_chunks_order'),
    )

# Create tables
def init_db():
    """Initialize database with proper extensions"""
    with engine.connect() as conn:
        # Enable UUID extension for PostgreSQL
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"))
        conn.commit()
    
    # Create all tables
    Base.metadata.create_all(bind=engine)

# Database dependency for FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()