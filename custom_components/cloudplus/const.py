"""Constants for the CloudPlus / Meari integration."""

DOMAIN = "cloudplus"

CONF_EMAIL = "email"
CONF_PASSWORD = "password"
CONF_DEVICE_ID = "device_id"
CONF_SN_NUM = "sn_num"
CONF_DEVICE_NAME = "device_name"
CONF_HOST_KEY = "host_key"
CONF_MOTION_TIMEOUT = "motion_timeout"

DEFAULT_MOTION_TIMEOUT = 120

# CloudEdge / Meari API
PARTNER_ID = "77"
TTID = "a"
SOURCE_APP = "77"
APP_VER = "5.9.2"
APP_VER_CODE = "1024"
PHONE_TYPE = "a"

DEFAULT_CA_KEY = "bc29be30292a4309877807e101afbd51"
DEFAULT_CA_SECRET = "35a69fd1-6527-4566-b190-921f9a651488"

DES_KEY = b"123456781234567812345678"
DES_IV = b"01234567"

REDIRECT_URL = "https://apis.cloudedge360.com"

# IoT Model codes for battery info
IOT_CODE_POWER_TYPE = "153"
IOT_CODE_BATTERY_PERCENT = "154"
IOT_CODE_BATTERY_REMAINING = "155"
IOT_CODE_CHARGE_STATUS = "156"
IOT_CODE_WIFI_SIGNAL = "1007"
BATTERY_CODES = "153,154,155,156,1007"

# Motion-related alarm types (from motion_detector.py)
MOTION_ALARM_TYPES = {1, 2, 11, 20}

ALARM_TYPE_NAMES = {
    1: "PIR",
    2: "Motion",
    3: "Visitor",
    6: "Noise",
    7: "Baby cry",
    8: "Face",
    9: "Call",
    10: "Tamper",
    11: "Human body",
    12: "Face detected",
    14: "Dog bark",
    17: "Cat",
    18: "Pet",
    19: "Package",
    20: "Person",
    21: "SD card removed",
    39: "Fire",
    41: "Cat meow",
}
