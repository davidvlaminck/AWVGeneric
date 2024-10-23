import json
from dataclasses import dataclass
from enum import Enum

from API.EMInfraDomain import TermDTO, BaseDataclass


class TestEnum(Enum):
    VALUE1 = 'value1'
    VALUE2 = 'value2'


@dataclass
class TestClass(BaseDataclass):
    prop_str: str | None = None
    prop_list_str: list[str] | None = None
    prop_int: int | None = None
    prop_bool: bool | None = None
    prop_object: object| None = None
    test_enum: TestEnum | None = None
    from_: str | None = None

    def __post_init__(self):
        self._fix_enums({('test_enum', TestEnum)})


@dataclass
class NestingTestClass(BaseDataclass):
    nested: TestClass | None = None
    nested_list: list[TestClass] | None = None

    def __post_init__(self):
        self._fix_nested_classes({('nested', TestClass)})
        self._fix_nested_list_classes({('nested_list', TestClass)})


def test_non_nested_class():
    t = TestClass(prop_str='test', prop_list_str=['test1', 'test2'], prop_int=1, prop_bool=True, prop_object=1.0,
                  test_enum=TestEnum.VALUE1)
    d = t.asdict()
    assert d == {'prop_str': 'test', 'prop_list_str': ['test1', 'test2'], 'prop_int': 1, 'prop_bool': True,
                           'prop_object': 1.0, 'test_enum': 'value1', 'from': None}
    assert t.json() == ('{"prop_str": "test", "prop_list_str": ["test1", "test2"], "prop_int": 1, "prop_bool": true, '
                        '"prop_object": 1.0, "test_enum": "value1", "from": null}')

    t_created = TestClass.from_dict(d)
    assert t_created == t


def test_reserved_prop():
    t = TestClass(from_='test')
    d = t.asdict()
    assert d == {'from': 'test', 'prop_str': None, 'prop_list_str': None, 'prop_int': None, 'prop_bool': None,
                           'prop_object': None, 'test_enum': None}


def test_nested_class():
    n = NestingTestClass(nested=TestClass(prop_str='test', prop_list_str=['test1', 'test2'], prop_bool=True))
    d = n.asdict()
    assert d == {'nested': {'from': None, 'prop_str': 'test',  'prop_list_str': ['test1', 'test2'], 'prop_int': None,
                            'prop_bool': True, 'prop_object': None, 'test_enum': None}, 'nested_list': None}
    assert n.json() == ('{"nested": {"prop_str": "test", "prop_list_str": ["test1", "test2"], "prop_int": null, '
                        '"prop_bool": true, "prop_object": null, "test_enum": null, "from": null}, "nested_list": null}')

    n_created = NestingTestClass.from_dict(d)
    assert n_created == n


def test_nested_class_list():
    t1 = TestClass(prop_str='test1')
    t2 = TestClass(prop_str='test2')
    n = NestingTestClass(nested_list=[t1, t2])
    d = n.asdict()
    assert d == {'nested': None, 'nested_list': [
        {'from': None, 'prop_str': 'test1',  'prop_list_str': None, 'prop_int': None,
         'prop_bool': None, 'prop_object': None, 'test_enum': None},
        {'from': None, 'prop_str': 'test2', 'prop_list_str': None, 'prop_int': None,
         'prop_bool': None, 'prop_object': None, 'test_enum': None} ]}
    assert n.json() == ('{"nested": null, "nested_list": [{"prop_str": "test1", "prop_list_str": null, '
                        '"prop_int": null, "prop_bool": null, "prop_object": null, "test_enum": null, "from": null}, '
                        '{"prop_str": "test2", "prop_list_str": null, "prop_int": null, "prop_bool": null, '
                        '"prop_object": null, "test_enum": null, "from": null}]}')

    n_created = NestingTestClass.from_dict(d)
    assert n_created == n
