import datetime

from dags.utils import cleaning_rules as cr


def test_standardize_student_id():
    res = cr.standardize_student_id("stu-002")
    assert res.value == "STU002"
    assert res.is_valid


def test_validate_email():
    assert cr.validate_email("John@Example.com").is_valid
    assert not cr.validate_email("bad@email").is_valid


def test_standardize_phone():
    assert cr.standardize_phone("9876543210").value == "+91-9876543210"
    assert not cr.standardize_phone("123").is_valid


def test_parse_date():
    res = cr.parse_date("15/05/1999")
    assert res.is_valid
    assert res.value == datetime.date(1999, 5, 15)


def test_validate_score():
    assert cr.validate_score(105).value == 100
    assert cr.validate_score(-10).value == 0
