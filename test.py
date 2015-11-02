from functools import update_wrapper

__author__ = 'sam'

def test_update_wrapper():
    def a():
        print 1
    def b():
        print 2
    update_wrapper(b, a)
    assert b.__name__ == a.__name__
    assert b.__module__ == a.__module__
    assert b.__doc__ == a.__doc__
    assert b.__dict__ == a.__dict__
