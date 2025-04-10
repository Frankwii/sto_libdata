from abc import ABC, abstractmethod
import re
from typing import Optional

class SQLDDLEntity(ABC):
    valid_name_regex = re.compile(r"^[A-Za-z_]+$")

    @property
    @abstractmethod
    def name(self)->str:
        ...

    @property
    @abstractmethod
    def parent(self)->Optional["SQLDDLEntity"]:
        ...

    def quote(self)->str:
        """
        Returns an appropriately quoted version of the name, only if necessary.
            
        Examples:
            "DIM_AÑO" |> "[DIM_AÑO]"
            "BSN"     |> "BSN"
        """
        return self.name if re.fullmatch(SQLDDLEntity.valid_name_regex, self.name) else "[" + self.name + "]"

    def get_raw_name(self)->str:
        return self.name

    def get_name(self)->str:
        return self.quote()

    def get_parent(self) -> Optional["SQLDDLEntity"]:
        return self.parent

    def get_full_name(self)->str:
        """
        Formats the full name of the object, including the parents' names with dots.
        """
        self_name = self.get_name()
        return self_name if not self.parent else self.parent.get_full_name()+"."+self_name

    def __str__(self):
        return self.get_full_name()

    def __repr__(self):
        return self.get_name()

class SQLSchema(SQLDDLEntity):
    def __init__(self, name:str):
        self._name = name

    @property
    def name(self)->str:
        return self._name

    @property
    def parent(self)->None:
        return None

class SQLTable(SQLDDLEntity):
    def __init__(self, name:str, parent:SQLSchema):
        self._name = name
        self._parent = parent

    @property
    def name(self)->str:
        return self._name

    @property
    def parent(self)->SQLSchema:
        return self._parent

    def get_schema_name(self)->str:
        return self.parent.get_name()

class SQLColumn(SQLDDLEntity):
    def __init__(self, name:str, parent:SQLTable):
        self._name = name
        self._parent = parent

    @property
    def name(self)->str:
        return self._name

    @property
    def parent(self)->SQLTable:
        return self._parent

    def get_schema_name(self)->str:
        return self.parent.get_schema_name()

    def get_table_name(self)->str:
        return self.parent.get_name()

    def get_table_full_name(self)->str:
        return self.parent.get_full_name()

class SQLType(ABC):
    @abstractmethod
    def get_name(self) -> str:
        ...

    @abstractmethod
    def format_to_query_string(self) -> str:
        """
        Formats self to a string that can be placed in a query as a type
        e.g. "INT" or "NVARCHAR(255)".
        """
        ...

    def __str__(self) -> str:
        return self.format_to_query_string()

class SQLParametrizedType(SQLType, ABC):
    """Type that requires parameters."""
    @property
    @abstractmethod
    def parameters(self) -> list[str]:
        ...

    def format_to_query_string(self) -> str:
        internal_parameters = ",".join(filter(lambda p: bool(p), self.parameters))

        formatted_parameters = "(" + internal_parameters + ")" if bool(internal_parameters) else ""

        return (self.get_name() + formatted_parameters).upper()

class SQLSimpleType(SQLParametrizedType, ABC):
    """Type that requires no parameters."""
    @property
    def parameters(self) -> list[str]:
        return []

class SQLIntType(SQLSimpleType):
    def get_name(self):
        return "INT"

class SQLDateType(SQLSimpleType):
    def get_name(self):
        return "DATE"

class SQLVarcharType(SQLParametrizedType):
    def get_name(self):
        return "VARCHAR"

    def __init__(self, size_in_bytes: str="MAX"):
        self._parameters = [size_in_bytes]

    @property
    def parameters(self):
        return self._parameters

class SQLNvarcharType(SQLParametrizedType):
    def get_name(self):
        return "NVARCHAR"

    def __init__(self, size_in_bytes: str="MAX"):
        self._parameters = [size_in_bytes]

    @property
    def parameters(self):
        return self._parameters

class SQLNcharType(SQLParametrizedType):
    def get_name(self):
        return "NCHAR"

    def __init__(self, size_in_bytes: str="MAX"):
        self._parameters = [size_in_bytes]

    @property
    def parameters(self):
        return self._parameters

class SQLCharType(SQLParametrizedType):
    def get_name(self):
        return "CHAR"

    def __init__(self, size_in_bytes: str="MAX"):
        self._parameters = [size_in_bytes]

    @property
    def parameters(self):
        return self._parameters

class SQLDatetimeType(SQLParametrizedType):
    def get_name(self):
        return "DATETIME2"

    def __init__(self, p: str = "", r: str = ""):
        self._parameters = [p, r]

    @property
    def parameters(self) -> list[str]:
        return self._parameters

if __name__=="__main__":
    dt = SQLDatetimeType()
    nv = SQLNvarcharType("20")
    print(nv)
