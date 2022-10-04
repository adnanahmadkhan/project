from main import raw_trades_data_path, raw_valuedata_path, read_raw_data, DATABASE
from utils import get_connection


def test_db():
    assert get_connection(DATABASE) is not None


def test_validate_data():
    tdata, vdata = read_raw_data(raw_trades_data_path, raw_valuedata_path)

    assert tdata.shape == (69050, 3)
    assert vdata.shape == (208800, 6)
