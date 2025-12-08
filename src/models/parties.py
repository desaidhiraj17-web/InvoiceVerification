from sqlalchemy import Column, String, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime
from src.db.database import Base


class PartyMaster(Base):
    __tablename__ = "party_master"

    id = Column(String, primary_key=True, index=True)
    party_code = Column(String, nullable=False)
    party_name = Column(String, nullable=False)
    party_gst = Column(String, nullable=True)
    party_address = Column(String, nullable=True)
    party_city = Column(String, nullable=True)
    active = Column(Boolean, default=True)

    updated_by = Column(String, ForeignKey("users.id",ondelete="SET NULL"), nullable=True)

    created_at = Column(
        String,
        default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    )
    updated_at = Column(
        String,
        default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        onupdate=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    )

    __table_args__ = (
        UniqueConstraint("party_code", "party_name", "party_address", name="uq_party_master_fields"),
    )

    # Relationships (optional)
    updater = relationship("User", back_populates="updated_parties", lazy="joined", uselist=False)
    invoices = relationship("Invoice", back_populates="party")