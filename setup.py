import setuptools, ast

with open('README.rst', 'r') as o:
    long_description = o.read()

with open('geocode_sparcs/_version.py', 'r') as o:
    version = ast.literal_eval(ast.parse(o.read()).body[0].value)

setuptools.setup(
    name = 'geocode_sparcs',
    version = version,
    author = 'Kodi B. Arfer',
    description = "Geocode addresses from New York State's SPARCS data",
    long_description = long_description,
    long_description_content_type = 'text/x-rst',
    project_urls = {
        'Source Code': 'https://github.com/justlab/geocode_sparcs'},
    install_requires = [
        'tqdm >= 4.65.0',
        'requests >= 2.28.2'],
    packages = setuptools.find_packages(),
    classifiers = [
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Operating System :: OS Independent'])
