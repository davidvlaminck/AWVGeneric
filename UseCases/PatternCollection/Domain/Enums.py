from enum import Enum


class Direction(Enum):
    NONE = 'None',
    WITH = 'With',
    REVERSED = 'Reversed'


class Toestand(str, Enum):
    IN_ONTWERP = 'IN_ONTWERP'
    GEPLAND = 'GEPLAND'
    GEANNULEERD = 'GEANNULEERD'
    IN_OPBOUW = 'IN_OPBOUW'
    IN_GEBRUIK = 'IN_GEBRUIK'
    VERWIJDERD = 'VERWIJDERD'
    OVERGEDRAGEN = 'OVERGEDRAGEN'
    UIT_GEBRUIK = 'UIT_GEBRUIK'
