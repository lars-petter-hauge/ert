from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, ForeignKey, PickleType
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.schema import UniqueConstraint


Base = declarative_base()


class Ensemble(Base):
    __tablename__ = "ensembles"

    id = Column(Integer, primary_key=True)
    name = Column(String)

    __table_args__ = (
        UniqueConstraint("name", name="_name_ensemble_id_"),
    )

    def __repr__(self):
        return "<Ensemble(name='{}')>".format(self.name)


class Realization(Base):
    __tablename__ = "realizations"

    id = Column(Integer, primary_key=True)
    index = Column(Integer)
    ensemble_id = Column(Integer, ForeignKey("ensembles.id"))
    ensemble = relationship("Ensemble", back_populates="realizations")

    __table_args__ = (
        UniqueConstraint("index", "ensemble_id", name="_index_ensemble_id_"),
    )

    def __repr__(self):
        return "<Realization(index='{}')>".format(self.index)


Ensemble.realizations = relationship(
    "Realization", order_by=Realization.id, back_populates="ensemble"
)


class Response(Base):
    __tablename__ = "responses"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    indexes = Column(PickleType)
    values = Column(PickleType)
    realization_id = Column(Integer, ForeignKey("realizations.id"))
    realization = relationship("Realization", back_populates="responses")

    __table_args__ = (
        UniqueConstraint("name", "realization_id", name="_name_realization_id_"),
    )

    def __repr__(self):
        return "<Response(name='{}', indexes='{}', values='{:.3f}', realization_id='{}')>".format(
            self.name, self.indexes, self.values, self.realization_id
        )


Realization.responses = relationship(
    "Response", order_by=Response.id, back_populates="realization"
)


class Parameter(Base):
    __tablename__ = "parameters"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    group = Column(String)
    value = Column(Float)
    realization_id = Column(Integer, ForeignKey("realizations.id"))
    realization = relationship("Realization", back_populates="parameters")

    __table_args__ = (
        UniqueConstraint("name", "group", "realization_id", name="_name_realization_id_"),
    )

    def __repr__(self):
        return "<Response(name='{}', indexes='{}', values='{:.3f}', realization_id='{}')>".format(
            self.name, self.indexes, self.values, self.realization_id
        )


Realization.parameters = relationship(
    "Parameter", order_by=Parameter.id, back_populates="realization"
)


class Observation(Base):
    __tablename__ = "observations"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    key_indexes = Column(PickleType)
    data_indexes = Column(PickleType)
    values = Column(PickleType)
    stds = Column(PickleType)

    __table_args__ = (UniqueConstraint("name", name="_name_realization_id_"),)

    def __repr__(self):
        return "<Observation(name='{}', key_index='{}', data_index='{}', value='{}', std='{}')>".format(
            self.name, self.key_indexes, self.data_indexes, self.values, self.stds
        )


Session = sessionmaker()
