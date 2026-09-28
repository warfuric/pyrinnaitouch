"""Parse a captured status payload into a RinnaiSystemStatus."""
import json

from pyrinnaitouch import RinnaiCapabilities, RinnaiSystemMode
from pyrinnaitouch.system_status import RinnaiSystemStatus


def get_test_json():
    """Fixture to return a sample JSON response str"""
    return '[{"SYST": {"CFG": {"MTSP": "N", "NC": "00", "DF": "N", "TU": "C", "CF": "1", "VR": "0183", "CV": "0010", "CC": "043", "ZA": " ", "ZB": " ", "ZC": " ", "ZD": " " }, "AVM": {"HG": "Y", "EC": "N", "CG": "Y", "RA": "N", "RH": "N", "RC": "N" }, "OSS": {"DY": "TUE", "TM": "16:45", "BP": "Y", "RG": "Y", "ST": "N", "MD": "C", "DE": "N", "DU": "N", "AT": "999", "LO": "N" }, "FLT": {"AV": "N", "C3": "000" } } },{"CGOM": {"CFG": {"ZUIS": "N", "ZAIS": "Y", "ZBIS": "Y", "ZCIS": "N", "ZDIS": "N", "CF": "N", "PS": "Y", "DG": "W" }, "OOP": {"ST": "F", "CF": "N", "FL": "00", "SN": "Y" }, "GSS": {"CC": "N", "FS": "N", "CP": "N" }, "APS": {"AV": "N" }, "ZUS": {"AE": "N", "MT": "999" }, "ZAS": {"AE": "N", "MT": "999" }, "ZBS": {"AE": "N", "MT": "999" }, "ZCS": {"AE": "N", "MT": "999" }, "ZDS": {"AE": "N", "MT": "999" } } }]'


def test_cooling_status_parses():
    status = RinnaiSystemStatus()
    assert status.handle_status(json.loads(get_test_json()))
    assert status.mode is RinnaiSystemMode.COOLING
    assert RinnaiCapabilities.HEATER in status.capabilities
    assert RinnaiCapabilities.COOLER in status.capabilities
    assert RinnaiCapabilities.EVAP not in status.capabilities
    assert status.firmware_version == "0183"
    assert not status.has_fault
    assert set(status.unit_status.zones) == {"A", "B"}


def test_unknown_unit_is_rejected_not_raised():
    assert RinnaiSystemStatus().handle_status([{"SYST": {}}, {"WAT": {}}]) is False
