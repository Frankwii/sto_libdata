from sqlalchemy import ForeignKey
from src.sto_libdata.dataframe_handling.normalization import _ForeignKeyStateHandler

type ForeignKeyDict = dict[str, dict[str, ForeignKey]]

def asssert_foreign_key_dicts_are_equal(
    dict1: ForeignKeyDict,
    dict2: ForeignKeyDict,
):
    tables1 = set(dict1.keys())
    tables2 = set(dict2.keys())

    assert tables1 == tables2, "Sets of tables are different!"

    for table in tables1:
        columns1 = set(dict1[table].keys())
        columns2 = set(dict2[table].keys())

        assert columns1 == columns2, f"Sets of columns in table {table} are different!"
        for column in columns1:
            assert repr(dict1[table][column]) == repr(dict2[table][column])


def test_foreign_key_addition():
    handler = _ForeignKeyStateHandler()

    handler.add_foreign_key("FAC_FLIGHT", "ID_ORIGIN_AIRPORT", "DIM_AIRPORT", "ID")
    handler.add_foreign_key("FAC_FLIGHT", "ID_DESTINATION_AIRPORT", "DIM_AIRPORT", "ID")
    handler.add_foreign_key("DIM_AIRPORT", "ID_CITY", "DIM_CITY", "ID")

    output_fks = handler.get_foreign_keys()

    expected_fks = {
        "FAC_FLIGHT": {
            "ID_ORIGIN_AIRPORT": ForeignKey("DIM_AIRPORT.ID"),
            "ID_DESTINATION_AIRPORT": ForeignKey("DIM_AIRPORT.ID")
        },
        "DIM_AIRPORT": {"ID_CITY": ForeignKey("DIM_CITY.ID")}
    }

    asssert_foreign_key_dicts_are_equal(output_fks, expected_fks)

def test_table_renaming():
    handler = _ForeignKeyStateHandler()

    handler.add_foreign_key("FAC_FLIGHT", "ID_ORIGIN_AIRPORT", "DIM_AEROPUERTO", "ID")
    handler.add_foreign_key("FAC_FLIGHT", "ID_DESTINATION_AIRPORT", "DIM_AEROPUERTO", "ID")
    handler.add_foreign_key("DIM_AEROPUERTO", "ID_CITY", "DIM_CITY", "ID")

    handler.rename_table("DIM_AEROPUERTO", "DIM_AIRPORT")

    output_fks = handler.get_foreign_keys()

    expected_fks = {
        "FAC_FLIGHT": {
            "ID_ORIGIN_AIRPORT": ForeignKey("DIM_AIRPORT.ID"),
            "ID_DESTINATION_AIRPORT": ForeignKey("DIM_AIRPORT.ID")
        },
        "DIM_AIRPORT": {"ID_CITY": ForeignKey("DIM_CITY.ID")}
    }

    asssert_foreign_key_dicts_are_equal(output_fks, expected_fks)

def test_column_renaming():
    handler = _ForeignKeyStateHandler()

    handler.add_foreign_key("FAC_FLIGHT", "ID_ORIGIN_AIRPORT", "DIM_AIRPORT", "ID")
    handler.add_foreign_key("FAC_FLIGHT", "ID_DESTINATION_AIRPORT", "DIM_AIRPORT", "ID")
    handler.add_foreign_key("DIM_AIRPORT", "ID_CIUDAD", "DIM_CITY", "ID")


    handler.rename_column("DIM_AIRPORT", "ID_CIUDAD", "ID_CITY")

    output_fks = handler.get_foreign_keys()

    expected_fks = {
        "FAC_FLIGHT": {
            "ID_ORIGIN_AIRPORT": ForeignKey("DIM_AIRPORT.ID"),
            "ID_DESTINATION_AIRPORT": ForeignKey("DIM_AIRPORT.ID")
        },
        "DIM_AIRPORT": {"ID_CITY": ForeignKey("DIM_CITY.ID")}
    }

    asssert_foreign_key_dicts_are_equal(output_fks, expected_fks)

def test_table_and_column_renaming():
    handler = _ForeignKeyStateHandler()

    handler.add_foreign_key("FAC_FLIGHT", "ID_ORIGIN_AIRPORT", "DIM_AEROPUERTO", "ID")
    handler.add_foreign_key("FAC_FLIGHT", "ID_DESTINATION_AIRPORT", "DIM_AEROPUERTO", "ID")
    handler.add_foreign_key("DIM_AEROPUERTO", "ID_CIUDAD", "DIM_CITY", "ID")

    handler.rename_column("DIM_AEROPUERTO", "ID_CIUDAD", "ID_CITY")
    handler.rename_table("DIM_AEROPUERTO", "DIM_AIRPORT")

    output_fks = handler.get_foreign_keys()

    expected_fks = {
        "FAC_FLIGHT": {
            "ID_ORIGIN_AIRPORT": ForeignKey("DIM_AIRPORT.ID"),
            "ID_DESTINATION_AIRPORT": ForeignKey("DIM_AIRPORT.ID")
        },
        "DIM_AIRPORT": {"ID_CITY": ForeignKey("DIM_CITY.ID")}
    }

    asssert_foreign_key_dicts_are_equal(output_fks, expected_fks)
