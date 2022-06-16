import yaml
import hashlib
import os
from distutils.version import LooseVersion

class ProductConfig:
    _new_product = { 'archive_type': None,
        'product': None,
        'archive': None,
        'archive_md5': None,
        'media_dir': None,
        'work_dir': None,
        'md5': None,
        'out': None,
        'archive_check': None,
        'version': None,
        'product_version': None,
        'import_version': None,
        'clone_url': None,
        'import_branch': None,
        'installed': None,
        'merged': None
    }

    def __init__(self):
        pass

    def product_prefixes(self, formatting="prefix"):
        if not formatting:
            formatting = "prefix"

        prefixes = {
            'cos': {
                'product': 'cos',
                'description': 'HPE Cray Operating System',
                },
            'SUSE-Backports-SLE': {
                'product': 'sles',
                'description': 'openSUSE packages backports provided by SUSE',
                },
            'SUSE-PTF': {
                'product': 'sles',
                'description': 'PTFs (Program Temporary Fixes) provided by SUSE',
                },
            'SUSE-Products': {
                'product': 'sles',
                'description': 'Base SLE software provided by SUSE',
                },
            'SUSE-Updates': {
                'product': 'sles',
                'description': 'Updates to base SLE software provided by SUSE',
                },
            'slingshot-host-software': {
                'product': 'slingshot-host-software',
                'description': 'Slingshot Host Software and Drivers',
                },
            # 'analytics': 'analytics',
            'uan': {
                'product': 'uan',
                'description': 'HPE Cray User Access Node (UAN) Software',
                },
            # 'cpe': 'cpe',
            # 'cpe-slurm': 'slurm',
            # 'wlm-slurm': 'slurm',
            # 'wlm-pbs': 'pbs',
            # 'cpe-pbs': 'pbs',
            # 'cray-sdu-rda': 'sdu'
            'sma': {
                'product': 'sma',
                'description': 'HPE Cray EX System Monitoring Application',
                },
            'sat': {
                'product': 'sat',
                'description': 'HPE Cray System Admin Toolkit',
                },
        }
 
        retval = dict()

        for prefix in prefixes:
            if formatting == "description":
                retval[prefix] = prefixes[prefix]['description']
            elif formatting == "product":
                retval[prefixes[prefix]['product']] = 1
            elif formatting == "prefix":
                retval[prefix] = prefixes[prefix]['product']
            else:
                raise InstallerError(f"Unknown prefix formatting {formatting}")

        if formatting == "product":
            return list(retval.keys())
        else:
            return retval

    def new_product(self):
        return self._new_product.copy()

class ProductCatalog:
    _all = None
    _data = None
    _version = None
    _short_version = None
    _key = None
    _name = None

    def __init__(self):
        self._data = dict()
        self._all = list()

    def insert(self, product):
        if product.name not in self._all:
            self._data[product.name] = product
            self._all.append(product.name)

    def __getitem__(self, x):
        return self._data[self._all[x]]

    def get_versions(self, installed=True):
        high = None
        short = None
        key = None

        versions = list()
        keys = list()
        for p in self:
            if installed:
                check = p.installed
            else:
                check = p.work_dir

            if check:
                if p.product_version:
                    versions.append(p.product_version)
                elif p.version:
                    versions.append(p.version)
                keys.append(p.name)

        sorted_vers = sorted(versions, key=LooseVersion)
        if sorted_vers:
            high = sorted_vers[-1]
            version_list = high.split('.')
            short = "{}.{}".format(version_list[0], version_list[1])

        sorted_keys = sorted(keys, key=LooseVersion)
        if sorted_keys:
            key = sorted_keys[-1]

        return high, short, key

    def update_versions(self):
        high, short, key = self.get_versions()
        self.version = high
        self.short_version = short
        self.key = key

    def best(self, installed=True):
        if not installed:
            high, short, key = self.get_versions(installed)
            return self._data[key]

        if not self._key:
            self.update_versions()

        if self._key:
            return self._data[self._key]

        return None

    @property
    def count(self):
        return len(self._all)

    @property
    def key(self):
        if not self._key:
            self.update_versions()

        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    @property
    def clone_url(self):
        if not self._key:
            self.update_versions()

        return self._data[self._key].clone_url

    @property
    def version(self):
        if not self._version:
            self.update_versions()

        return self._version

    @version.setter
    def version(self, value):
        self._version = value

    @property
    def short_version(self):
        if not self._short_version:
            self.update_versions()

        return self._short_version

    @short_version.setter
    def short_version(self, value):
        self._short_version = value

class Product:
    def __init__(self, name, config, observer=None):
        self.__dict__[name] = config.new_product()
        self.__dict__["name"] = name
        if observer:
            self.__dict__["observer"] = observer

    def __repr__(self):
        return repr(self.__dict__)

    def __setattr__(self, name, value):
        self.__dict__[self.__dict__["name"]][name] = value

        if "observer" in self.__dict__:
            self.__dict__["observer"](self)

    def __getattr__(self, name):
        return self.__dict__[self.__dict__["name"]].get(name, None)

    def __str__(self):
        return self.yaml()

    @property
    def best_version(self):
        if self.import_version:
            return self.import_version
        elif self.product_version:
            return self.product_version
        else:
            return self.version

    def yaml(self):
        curdict = self.__dict__.copy()
        del curdict["name"]
        del curdict["observer"]
        return yaml.dump(curdict)

    def get_dict(self):
        curdict = self.__dict__.copy()
        del curdict["name"]
        del curdict["observer"]
        return curdict

    @property
    def archive_check(self):
        ac = self.__dict__[self.__dict__["name"]]['archive_check']
        if not ac or ac == 'skipped':
            if self.md5 and self.archive_md5:
                if self.archive_md5 == self.md5:
                    self.archive_check = 'passed'
                else:
                    self.archive_check = 'failed'
            else:
                self.archive_check = 'skipped'

        return self.__dict__[self.__dict__["name"]]['archive_check']

    @archive_check.setter
    def archive_check(self, value):
        self.__dict__[self.__dict__["name"]]['archive_check'] = value

    @property
    def archive_md5(self):
        if not self.__dict__[self.__dict__["name"]]['archive_md5']:
            media_dir = self.__dict__[self.__dict__["name"]]['media_dir']
            archive = self.__dict__[self.__dict__["name"]]['archive']

            if media_dir and archive:
                hash_md5 = hashlib.md5()
                filename = os.path.join(media_dir,archive)
                with open(filename, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)

                self.archive_md5 = hash_md5.hexdigest()

        return self.__dict__[self.__dict__["name"]]['archive_md5']

    @archive_md5.setter
    def archive_md5(self, value):
        print("setting archive_md5 to", value)
        self.__dict__[self.__dict__["name"]]['archive_md5'] = value

class Products:
    config = ProductConfig()
    _catalog = None

    def __getitem__(self, x):
        return self.__dict__[self.all_products[x]]

    def __init__(self, location_dict=None, dryrun=False):
        self._initialize_catalog()
        self.dryrun = dryrun
        self.installable_products = list()
        self.uninstallable_products = list()
        self.all_products = list()
        self.products = dict()
        self.location_dict = location_dict
        self.initialized = False

        self.load_location_dict()

    def __repr__(self):
        masterdict = dict()
        for product in self.all_products:
            masterdict.update(self.__dict__[product].get_dict())

        return repr(masterdict)

    def __str__(self):
        return self.yaml()

    def _initialize_catalog(self):
        self._catalog = dict()
        for product in self.prefixes('product'):
            self._catalog[product] = ProductCatalog()

    def get(self, name, create=True):
        if name not in self.__dict__:
            if not create:
                return None
            self.__dict__[name] = Product(name, self.config, self._update_products)
            self.all_products.append(name)

        return self.__dict__[name]

    def load_location_dict(self, encoding='UTF-8'):
        location_dict = self.location_dict
        if not location_dict or not os.path.exists(location_dict):
            return

        with open(location_dict, "r", encoding=encoding) as f:
            ldict = yaml.full_load(f)

            for lprod in ldict:
                product = self.get(lprod)
                for attr in ldict[lprod]:
                    setattr(product, attr, ldict[lprod][attr])

            self.initialized = True

    def _update_installable(self, product):
        if product.product and product.work_dir:
            if product.name in self.uninstallable_products:
                self.uninstallable_products.remove(product.name)
            if product.name not in self.installable_products:
                self.installable_products.append(product.name)
        else:
            if product.name in self.installable_products:
                self.installable_products.remove(product.name)
            if product.name not in self.uninstallable_products:
                self.uninstallable_products.append(product.name)

    def _update_products(self, product):
        self.initialized = True
        if product.product:
            self.product(product.product).insert(product)

        self._update_installable(product)
        #self._update_version(product)
        self.write_location_dict()

    def prefixes(self, formatting=None):
        return self.config.product_prefixes(formatting)

    def product(self, name):
        if name not in self._catalog:
            raise Exception(f"Invalid product type {name}")

        return self._catalog[name]

    def write_location_dict(self, encoding='UTF-8'):
        if self.dryrun or not self.location_dict:
            return
        data = self.yaml()
        with open(self.location_dict, "w", encoding=encoding) as f:
            f.write(data)

    def yaml(self):
        masterdict = dict()
        for product in self.all_products:
            masterdict.update(self.__dict__[product].get_dict())

        return yaml.dump(masterdict)
