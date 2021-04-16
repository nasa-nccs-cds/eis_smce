import os, logging
import traitlets.config as tlc


class EISBase(tlc.Configurable):
    """Common behaviours for plugins in this repo"""
    logger = None
    _default_cache_dir = tlc.Unicode(os.path.expanduser("~/.eis_smce/cache")).tag(config=True)

    def __init__(self, **kwargs ):
        super(EISBase, self).__init__(**kwargs)
        self.cache_dir = kwargs.pop('cache_dir', self._default_cache_dir )
        os.makedirs( self.cache_dir, exist_ok=True )
        self.setup_logging()

    def setup_logging(self):
        if EISBase.logger is None:
            EISBase.logger = logging.getLogger('eis_smce.intake')
            EISBase.logger.setLevel(logging.DEBUG)
            log_file = f'{self.cache_dir}/logging/eis_smce.log'
            os.makedirs( os.path.dirname(log_file), exist_ok=True )
            fh = logging.FileHandler( log_file )
            fh.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.ERROR)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            EISBase.logger.addHandler(fh)
            EISBase.logger.addHandler(ch)


class EISSingleton(EISBase):
    """A configurable that only allows one instance.

    This class is for classes that should only have one instance of itself
    or *any* subclass. To create and retrieve such a class use the
    :meth:`SingletonConfigurable.instance` method.
    """

    _instance = None

    @classmethod
    def _walk_mro(cls):
        """Walk the cls.mro() for parent classes that are also singletons

        For use in instance()
        """

        for subclass in cls.mro():
            if issubclass(cls, subclass) and \
                    issubclass(subclass, EISSingleton) and \
                    subclass != EISSingleton:
                yield subclass

    @classmethod
    def clear_instance(cls):
        """unset _instance for this class and singleton parents.
        """
        if not cls.initialized():
            return
        for subclass in cls._walk_mro():
            if isinstance(subclass._instance, cls):
                # only clear instances that are instances
                # of the calling class
                subclass._instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        """Returns a global instance of this class.

        This method create a new instance if none have previously been created
        and returns a previously created instance is one already exists.

        The arguments and keyword arguments passed to this method are passed
        on to the :meth:`__init__` method of the class upon instantiation.

        Examples
        --------
        Create a singleton class using instance, and retrieve it::

            >>> from traitlets.config.configurable import SingletonConfigurable
            >>> class Foo(SingletonConfigurable): pass
            >>> foo = Foo.instance()
            >>> foo == Foo.instance()
            True

        Create a subclass that is retrived using the base class instance::

            >>> class Bar(SingletonConfigurable): pass
            >>> class Bam(Bar): pass
            >>> bam = Bam.instance()
            >>> bam == Bar.instance()
            True
        """
        # Create and save the instance
        if cls._instance is None:
            inst = cls(*args, **kwargs)
            # Now make sure that the instance will also be returned by
            # parent classes' _instance attribute.
            for subclass in cls._walk_mro():
                subclass._instance = inst

        if isinstance(cls._instance, cls):
            return cls._instance
        else:
            raise Exception(
                "An incompatible sibling of '%s' is already instanciated"
                " as singleton: %s" % (cls.__name__, type(cls._instance).__name__)
            )

    @classmethod
    def initialized(cls):
        """Has an instance been created?"""
        return hasattr(cls, "_instance") and cls._instance is not None