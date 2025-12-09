import datetime

from dags.utils import transformations as tf


def test_age_group():
    assert tf.age_group(20) == "18-22"
    assert tf.age_group(24) == "23-27"
    assert tf.age_group(33) == "28-35"
    assert tf.age_group(40) == "35+"


def test_enrollment_fields():
    date_val = datetime.date(2023, 5, 10)
    result = tf.enrollment_fields(date_val)
    assert result["enrollment_month"] == 5
    assert result["enrollment_quarter"] == 2


def test_map_payment_status():
    assert tf.map_payment_status("paid") == "COMPLETED"
    assert tf.map_payment_status("unknown") == "UNKNOWN"
