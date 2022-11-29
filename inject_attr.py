"""
This simple mini-framework is built for interactive notebook workflows where the
bulk of the user's code relies on large, object-oriented, external libraries
such as pandas. Its purpose is to enable/enforce consistent, readable, safe, and
concise code when writing user-defined functions that interact with large, complex
classes imported from external libraries.

The goal of this project is simple: allow the user to directly modify imported
classes within their code in a concise, expressive, safe, and readable way,
without subclassing.

Its primary use-case is dataframe-centric workflows (pandas, pyspark, polars, etc.)
in which method-chaining is often desired over standalone function calls. Most of
the examples presented throughout this module use ``pandas``, but the concepts apply
to any similar package.

*The problem:*
------------

Defining custom functions is a critical step in any workflow. Unfortunately, in a
dataframe-centric, notebook workflow described above, standalone custom functions
can often make code messier than desired. And sometimes we avoid creating a
desired function because the resulting break in our pretty method-chained code
formatting outweighs the potential saved space, or because it would require
creating unnecessary extra variables.

Sometimes it's just nice to change how an object works.

>>> import pandas as pd
>>> from numpy import NaN
>>> df = pd.DataFrame({'a': [1,2,3], 'b': [4,5,NaN], 'c': [10,20,30]})
>>> sr = pd.Series([1,2,3,4,5])

Say you want ``pd.DataFrame`` and ``pd.Series`` to have an ``nrows`` property.

>>> @property
>>> def nrows(self):
>>>     return self.shape[0]
>>> setattr(pd.DataFrame, 'nrows', nrows)
>>> setattr(pd.Series, 'nrows', nrows)
>>> df.nrows
3

There are serious problems with this code:
1.  The reader doesn't know which class ``nrows`` will be a property of until they
    reach the bottom of the declaration. In larger code blocks, this is annoying.
2.  The ``setattr()`` calls are repetitive and occupy more space than the function!
3.  It's redundant, hard to refactor, and creates room for error. The function
    name is written 5 times, instead of 1.

Instead...

>>> @inject(pd.DataFrame, pd.Series)
>>> @property
>>> def nrows(self):
>>>     return self.shape[0]

``inject`` is a versatile decorator function that can be placed over a function OR
class definition. When placed above a class definition, ALL custom attributes
defined in the class, including class variables, will be moved to the targets
and deleted from the decorated class by default.

>>> @inject(pd.DataFrame)
>>> class _:
>>>     def print_cols(self):
>>>         print(list(self.columns))
>>>     def print_cols_reversed(self):
>>>         print(list(reversed(self.columns)))
>>> df.print_cols()
['a', 'b', 'c']

When quickly defining attributes under an injected class,
it can be easy to unknowingly replace an existing attribute. By default, ``Inject``
prevents this mistake. In a regular python environment (.py files), we could just
check ``hasattr()`` and raise an exception. In a notebook environment, however,
this feature would quickly break, as soon as a cell is re-run.  ``Inject`` maintains
a history of each injected attribute in your environment, using
(module name, target class, func name) as keys.

>>> @inject(pd.DataFrame)
>>> def shape(self):
>>>     ...
Traceback (most recent call last):
 ...
ExistingAttributeError: pandas.core.frame.DataFrame.shape already exists.
Pass `overwrite = True` to avoid this error.

Bypass this error with ``overwrite=True``

>>> @inject(pd.DataFrame, overwrite=True)
>>> def shape(self):
>>>     ...


Notes
-----
In the context of this framework, the concept of "injection" should NOT be confused
with dependency injection, an entirely different idea. In this framework, to *inject*
simply means to set an attribute on a class.

"""


import inspect
import types
from typing import TypeVar, Type, Callable, Iterable

T = TypeVar('T', bound='Inject') # generic



class ExistingAttributeError(Exception):
    """
    Used to prevent accidental replacement of existing attributes.
    Raised when the user has chosen to prevent replacement (``overwrite = False``),
    and the attribute hasn't yet been recorded (which means it's not user-defined)
    """
    pass


class Inject:
    """
    This class serves 3 purposes:
    1.  Contains all the methods used for injecting custom attributes into target classes
    2.  Keeps global record (``history``) of injected attributes (see below explanation)
    3.  Custom subclassing logic: Upon definition of a subclass, all of its user-defined
        attributes will be added as attributes to the specified target class(es)

    -   Class variable ``history`` lets notebook users enforce ``overwrite=False``
        without throwing errors upon repeated execution of cells. When
        ``overwrite=False``, an error is thrown when the target class already
        has an attribute with the same name as one that is to be injected.
        Keeping a historical record allows for previously added attributes to be
        ignored by the checks incurred by ``overwrite=False``

    
    Examples
    --------

    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': [1,2], 'b': [3,4]})
    >>> sr = pd.Series([1,2,3])


    Inject the contents of a class into multiple targets

    >>> class _(Inject, to = [pd.DataFrame, pd.Series]):
    >>>     SOME_CONSTANT = 5
    >>>     def print_shape(self):
    >>>         print(self.shape)
    >>>     @property
    >>>     def fav_color(self):
    >>>         return 'yellow'
    >>>     @classmethod
    >>>     def some_classmethod(cls):
    >>>         print('Here is some classmethod')
    >>> df.print_shape()
    (0, 0)
    >>> print(df.fav_color)
    yellow
    >>> pd.Series.some_classmethod()
    here is some classmethod
    >>> print(pd.Series.SOME_CONSTANT)
    5

    Validation to avoid accidentally/unknowingly replacing existing attrs

    >>> class _(Inject, to = pd.DataFrame):
    >>>     @property
    >>>     def shape(self):
    >>>         return "not the shape"
    Traceback (most recent call last):
     ...
    ExistingAttributeError: pandas.core.frame.DataFrame.shape already exists.
    Pass `overwrite = True` to avoid this error.

    History tracking to avoid unwanted ``ExistingAttributeError`` in repeated
    execution of notebook cells 

    >>> class _(Inject, to = pd.DataFrame):
    >>>     def something(self):
    >>>         return "something"
    >>> class _(Inject, to = pd.DataFrame):
    >>>     def something(self): # without tracking history, this would throw error
    >>>         return "something"

    Bypass this error with ``overwrite=True``. Also, 

    >>> class _(Inject, to = pd.DataFrame, overwrite=False):
    >>>     @property
    >>>     def shape(self):
    >>>         return "Not actually the shape"
    >>> print(df.shape)
    not actually the shape

    Raise
    -----
    ExistingAttributeError:
        When an uncached attribute already exists on target, and ``overwrite = False``

    """

    history = set()


    def __init__(self, *args, **kwargs) -> None:
        raise ValueError("Do not create an instance of this class")


    def __init_subclass__(cls, **kwargs) -> None:
        """
        A subclass of Inject will automatically be processed the same as if it were
        decorated with ``@inject()``. This is the most concise way to inject a group
        of functions under a class. Pass arguments for ``@inject()`` as class-level
        keyword arguments in the class definition, like so:

        >>> class _(Inject, to = [pd.DataFrame, pd.Series], overwrite=True):
        >>>     pass

        """
        return Inject.inject_class_contents(from_cls=cls, **kwargs)
    

    @classmethod
    def inject_class_contents(
        cls,
        from_cls: type,
        to: list | tuple | type,
        overwrite: bool = False,
        del_old_attrs: bool = True,
        as_property: bool = False,
    ) -> None:
        """
        Sets each custom attribute of ``from_cls`` as an attribute of the target
        class, for each target class in ``to``

        Parameters
        ----------
        from_cls : type
            Class containing attributes to be injected.
        to : list or tuple or type
            Target class(es) for injection. Plural version of ``target_cls``. Flexible
            data type to accomodate a variety of callers. Gets converted to ``list``
        overwrite : bool, default False
            Allow replacement of attribute that already exists in target class
            and wasn't already defined by the user. Default is False to serve as a
            safeguard against *accidental* changes
        del_old_attrs : bool, default True
            Delete all custom attributes from ``from_cls`` after injection
        as_property : bool, default False
            When True, all attributes in ``from_cls`` will be converted to ``property``
            type before injection. Therefore, all attributes *must* be of type ``function``
            or ``property``. This is useful when injecting a large quantity of
            functions, as a means of saving space by eliminating the need for repetitive
            ``@property`` decorators placed above each function. (Note: a ``@property``
            decorator will still be required for any elements that are followed by
            corresponding ``@<func>.setter`` or ``@<func>.deleter`` methods, as
            the python interpreter will try to evaluate them before the original
            getter function is converted to a property)
        """

        if isinstance(to, tuple):
            target_classes = list(to)
        elif isinstance(to, type):
            target_classes = [to]
        else:
            target_classes = to

        filtered_cls_dict = {
                k:v for k,v in from_cls.__dict__.items() 
            if k not in ["__weakref__", "__dict__", "__module__", "__doc__"]
        }

        for attr_name, attr_obj in filtered_cls_dict.items():
            for target_cls in target_classes:
                cls._inject_attr_and_cache(
                    target_cls=target_cls,
                    attr_name=attr_name,
                    attr_obj=attr_obj,
                    overwrite=overwrite,
                    as_property=as_property,
                )
            if del_old_attrs:
                delattr(from_cls, attr_name)
    

    @classmethod
    def _inject_attr_and_cache(
        cls,
        target_cls: type,
        attr_name: str,
        attr_obj: any,
        overwrite: bool,
        as_property: bool,
    ) -> None:
        """
        Validate and process request to inject attribute, and record history.

        Parameters
        ----------
        target_cls : type
            Target class into which ``attr_obj`` will be injected
        attr_name : str
            Name that ``target_cls`` will store ``attr_obj`` under
        attr_obj : any
            Attribute to inject into ``target_cls``
        overwrite : bool, default False
            Allow replacement of attribute that already exists in target class
            and wasn't defined by the user. Default is False to serve as a
            safeguard against *accidental* changes
        as_property : bool, default False
            When True, ``attr_obj`` will be converted to ``property`` type before
            injection. When True, ``attr_obj`` *must* be of type ``function`` or ``property``.
        
        Notes
        -----
        If ``as_property = True``, an error will be raised if ``attr_obj`` is not
        a valid function that takes a single positional parameter.
        
        Raise
        -----
        TypeError:
            When ``as_property = True`` and ``attr_obj`` is not a valid function
        ValueError:
            When ``as_property = True`` and ``attr_obj`` takes invalid number of params (!= 1)
        ExistingAttributeError:
            When an uncached attribute already exists on target, and ``overwrite = False``
        """
        if as_property == True and not isinstance(attr_obj, property):
            if not inspect.isfunction(attr_obj):
                raise TypeError(
                    f"Can't convert '{attr_name}', of type '{type(attr_obj)}', to a property"
                )
            if 1 != (num_params := len(inspect.getfullargspec(attr_obj).args)):
                raise ValueError(
                    f"Function '{attr_name}' must require exactly 1 positional argument for it to "
                    f"be converted to a property. It's defined to take {num_params}."
                )
            attr_obj = property(attr_obj)


        if overwrite == False:
            cls._validate_non_existing_attribute(target_cls=target_cls, attr_name=attr_name)

        cls.history.add(cls._make_history_key(target_cls, attr_name))
        setattr(target_cls, attr_name, attr_obj)


    @classmethod
    def _validate_non_existing_attribute(
        cls, target_cls: type, attr_name: str,
    ) -> None:
        """
        Responsible for raising friendly ``ExistingAttributeError`` when target
        class already has a desired attribute not already added by the user
        
        Parameters
        ----------
        target_cls : type
            Target class into which the attribute will be injected
        attr_name : str
            Name that ``target_cls`` will store the new attribute under
        
        Raise
        -----
        ExistingAttributeError:
            When an uncached attribute already exists on target, and ``overwrite = False``
        """
        if (
            hasattr(target_cls, attr_name) and
            (history_key := cls._make_history_key(target_cls, attr_name)) not in cls.history
        ):
            raise ExistingAttributeError(
                f"{'.'.join([k if k is not None else '[Unknown]' for k in history_key])} "
                "already exists. Pass `overwrite = True` to avoid this error."
            ) 
    

    @staticmethod
    def _make_history_key(
        target_cls: type, attr_name: str
    ) -> tuple:
        """
        Since ``Inspect.history`` persists globally, a robust unique identifier
        for each history record is necessary: (module name, class name, attr name)

        Examples
        --------
        >>> import pandas as pd
        >>> @inject(pd.DataFrame)
        >>> def foo(self):
        >>>     ...
        >>> Inject.history
        {('pandas.core.frame', 'DataFrame', 'foo')}

        Notes
        -----
        First element in key will be ``None`` if ``inspect.getmodule()`` returns None

        """

        return (
            module.__name__ if (module := inspect.getmodule(target_cls)) is not None else None,
            target_cls.__name__,
            attr_name
        )
    

    @classmethod
    def get_subclass(
        cls, from_cls: type, **kwargs
    ) -> Type[T]:
        """
        Alternative to calling ``cls.inject_class_contents()`` directly.
        Given a class and kwargs, this dynamically creates and returns a new
        class that inherits from Inject, invoking its ``__init_subclass__()``,

        Parameters
        ----------
        from_cls : type
            Class containing attributes to be injected.
        kwargs
            Keyword arguments to be passed as class-level keyword arguments to
            the new class definition, to then be received by ``cls.__init_subclass__()``
        """

        updated_cls_dict = {
                k:v for k,v in from_cls.__dict__.items() 
            if k not in ["__weakref__", "__dict__"]
        }

        return types.new_class(
            from_cls.__name__,
            bases=(Inject,),
            kwds = kwargs,
            exec_body = lambda body: (body.update(updated_cls_dict))
        )



def inject(
    *args: tuple[type, ...] | tuple[Iterable[type]],
    overwrite: bool = False,
    del_old_attrs: bool = True,
    as_property: bool = False,
) -> Callable:
    """
    Inject the decorated element (or the elements in a decorated class)
    into selected classes. Accepts either a function, property, or a class.

    If decorating a property directly, the ``@property`` syntax must be
    used (not ``property()``), and the ``@property`` decorator must be
    placed below ``@inject()``

    If decorating a class, all custom attributes of the class, (including
    class variables, property setter/deleters, etc.) will be injected,
    and, by default, deleted from the decorated class. This can be disabled
    with ``del_old_attrs = False``

    Parameters
    ----------
    args : tuple[type, ...] or tuple[list[type] | tuple[type, ...]]
        Target class(es) to inject the decorated element into
    overwrite : bool, default False
        Allow replacement of attribute that already exists in target class
        and wasn't already defined by the user. Default is False to serve as a
        safeguard against *accidental* changes
    del_old_attrs : bool, default True
        Only has effect when the decorated element is a class
        Deletes all custom attributes from decorated class after injection
    as_property : bool, default False
        When True, attribute(s) will be converted to ``property`` type before
        injection. Passed attributes *must* be of type ``function`` or ``property``.
        This is useful when injecting a class containing a large quantity of
        functions, as a means of saving space by eliminating the need for repetitive
        ``@property`` decorators placed above each function. (Note: a ``@property``
        decorator will still be required for any elements that are followed by
        corresponding ``@<func>.setter`` or ``@<func>.deleter`` methods, as
        the python interpreter will try to evaluate them before the original
        getter function is converted to a property)

    Examples:
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': [1,2], 'b': [3,4]})


    Inject a single function to multiple target classes

    >>> @inject(pd.DataFrame, pd.Series)
    >>> def say_hi(self):
    >>>     return 'hi'
    >>> df.say_hi() # can be called on Series too
    hi

    Inject the entire contents of a class

    >>> @inject(pd.DataFrame)
    >>> class _:
    >>>     SOME_CONSTANT = 5
    >>>     @property
    >>>     def say_hi(self):
    >>>         return 'hi'
    >>> df.say_hi
    hi
    >>> pd.DataFrame.SOME_CONSTANT
    5

    A more concise but less intuitive alternative to decorating a class with
    ``inject()`` is to define a class which inherits from ``Inject`` and pass
    all necessary arguments as class-level keyword arguments

    For instance, this code ...
    >>> @inject(pd.DataFrame)
    >>> class _: ...


    can also be written as ...
    >>> class _(Inject, to = pd.DataFrame): ...

    
    Notes
    -----
    Will NOT work for decorating property setter/deleters, or class variables
    directly. To inject such types, decorate a class which contains them

    """
    if len(args) == 1 and (isinstance(args[0], tuple) or isinstance(args[0], list)):
        args = tuple(args[0])

    def inner(e):

        if inspect.isclass(e):
            Inject.inject_class_contents(from_cls=e, to=args, overwrite=overwrite, as_property=as_property)

        elif inspect.isfunction(e) or isinstance(e, property):
            name = e.fget.__name__ if isinstance(e, property) else e.__name__
            for cls in args:
                Inject._inject_attr_and_cache(
                    target_cls=cls,
                    attr_name=name,
                    attr_obj=e,
                    overwrite=overwrite,
                    as_property=as_property,
                )
        else:
            raise ValueError("Don't know how to handle this")
    return inner