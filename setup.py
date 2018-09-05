from pathlib import Path
from setuptools import setup

about_file = Path(Path(__file__).parent, 'flask_sqlalchemy_pgevents', '__about__.py')

about = {}
with open(about_file) as f:
    exec(f.read(), about)

setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__summary__'],
    author=about['__author__'],
    author_email=about['__email__'],
    url=about['__uri__'],
    license=about['__license__'],
    packages=['flask_sqlalchemy_pgevents'],
    zip_safe=False,
    platforms='any',
    install_requires=[
        'Flask>=1.0.2',
        'Flask-SQLAlchemy>=2.3.2',
        'psycopg2-binary>=2.7.5',
        # TODO: add psycopg2-pgevents once it is publishws

    ],
    tests_require=[
        'pytest',
        'pytest-cov'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ]
)
