from enum import Enum


class Environment(Enum):
    PRD = 'prd',
    DEV = 'dev',
    TEI = 'tei',
    AIM = 'aim'


class AuthType(Enum):
    JWT = 'JWT',
    CERT = 'cert',
    COOKIE = 'cookie'