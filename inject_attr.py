import re

'''
Allows the user to define custom functions/methods
to be injected into an externally imported class,
like pandas DataFrame or Series
--

Examples:
----------

>>> import pandas as pd
>>> def bar(self, text):
>>>     """ ::pd.DataFrame,pd.Series
>>>     This function does other stuff
>>>     """
>>>     print(text)
>>> 
>>> _add_func_to_class('bar')
>>> 
>>> sr = pd.Series()
>>> df = pd.DataFrame()
>>> df.bar('hi')
hi
>>> sr.bar('hello')
hello

>>> @inject(pd.DataFrame)
>>> def say_hi(self):
>>>     print("hi from dataframe")
>>> 
>>> @inject(pd.Series)
>>> def say_hi(self):
>>>     print("hi from series")
>>> 
>>> df = pd.DataFrame()
>>> sr = pd.Series()
>>> 
>>> df.say_hi()
hi from dataframe
>>> sr.say_hi()
hi from series

'''



class ExistingAttributeError(Exception):
    pass


def inject(*args, overwrite=True):
    """
    Injects its decorated function into a class of choice.
    Use this as a decorator and pass any target classes as positionals
    --
    THIS CHOICE HAS LIMITATIONS!
    Will not work for any properties, classmethods, staticmethods, or any other
    decorated function. It will also not work for class variables. If you need
    such functionality, use the `Inject` class instead.

    Examples:
    --------
    >>> import pandas as pd
    >>> #
    >>> @inject(pd.DataFrame) # Accepts multiple objects as positionals
    >>> def say_hi(self):
    >>>     return 'hi'
    >>> #
    >>> df = pd.DataFrame()
    >>> df.say_hi()
    hi
    """
    def inner(func):
        for cls in args:
            if overwrite == False and hasattr(cls, func.__name__):
                raise ExistingAttributeError(
                        f"Class '{cls.__name__}' already has attribute, '{func.__name__}'. "
                        "Pass `overwrite = True` to avoid this error."
                    )
            setattr(cls, func.__name__, func)
    return inner



class Inject:
    """
    A magical, unusual class.
    Upon definition of a class that inherits from Inject, all of its
    user-defined attributes will be 'moved' to the target class(es)
    specified in `to`, being set as attributes of the target and
    removed as attributes from the declarer.


    - `cache` is used to let notebook users re-run the same cell
      while using overwrite=False without throwing errors. When
      `overwrite=False`, we throw an error whenever the target
      class already has the attribute we're trying to inject.
      By caching our previously injected attributes, this check
      is ignored for them.

    
    Examples:
    --------

    >>> import pandas as pd
    >>> #
    >>> class _(Inject, to = pd.DataFrame):
    >>>     LOL = 'lol'
    >>>     #
    >>>     def print_shape(self):
    >>>         print(self.shape)
    >>>     #
    >>>     @property
    >>>     def fav_color(self):
    >>>         return 'yellow'
    >>>     #
    >>>     @classmethod
    >>>     def some_classmethod(cls):
    >>>         print('Here is some classmethod')
    >>>     #
    >>>     #
    >>> df = pd.DataFrame()
    >>> df.print_shape()
    (0, 0)
    >>> print(df.fav_color)
    yellow
    >>> pd.DataFrame.some_classmethod()
    here is some classmethod
    >>> print(pd.DataFrame.LOL)
    lol


    >>> class _(Inject, to = [pd.Series, pd.DataFrame], overwrite=True):
    >>>     #
    >>>     @property
    >>>     def shape(self): # If `overwrite` were False, this would throw error
    >>>         return "Not actually the shape lol"
    >>>     #
    >>> df = pd.DataFrame()
    >>> print(df.shape)
    not actually the shape lol
    >>> sr = pd.Series()
    >>> print(sr.shape)
    not actually the shape lol

    """

    cache = []

    def __init__(self, *args, **kwargs):
        raise ValueError("Do not create an instance of this class")

    def __init_subclass__(
                cls,
                *,
                to: list or any,
                overwrite:bool = False
            ):

        if not isinstance(to, list):
            to = [to]

        attrs = [
                    (k,v) for k,v in cls.__dict__.items()
                if k not in ['__module__', '__doc__']
            ]

        for name, attr in attrs:
            for target in to:
                if overwrite == False and hasattr(target, name) and name not in Inject.cache:
                    raise ExistingAttributeError(
                            f"Class '{target.__name__}' already has attribute, '{name}'. "
                            "Pass `overwrite = True` to avoid this error."
                        )
                setattr(target, name, attr)
                Inject.cache.append(name)
                delattr(cls, name)




def inject_all_attrs():
    """
    To use this, you must exec this file to define the function in local scope

    >>> import pandas as pd
    >>> execfile('method_injector.py') # this file
    >>> #
    >>> def say_hi(self):
    >>>     ''' ::pd.DataFrame '''
    >>>     print('hello')
    >>> #
    >>> inject_all_attrs()
    >>> #
    >>> df = pd.DataFrame()
    >>> df.say_hi()
    hello
    """
    for name,func in list(globals().items()):
        if callable(func) or isinstance(func, property):
            # To define implementations of the same function on multiple
            # classes, all underscores at the end of each function will be removed
            name = name.rstrip('_')
            doc = func.__doc__
            if doc:
                # Find classes inside docstring with matching format
                objects = re.findall(r'^\s*:{2}(\S+)', doc)
                if len(objects) > 0:
                    objects = objects[0].split(',')
                    # Separate classes by comma
                    for obj in objects:
                        setattr(eval(obj), name, func)
