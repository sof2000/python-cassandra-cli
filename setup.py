import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f: 
    requirements = f.readlines() 

setuptools.setup(
    name="python-cassandra-cli",
    version="1.4.0",
    author="Andrii Derevianko",
    author_email="sof2025@gmail.com",
    description="Python CLI for cassandra snapshots. CLI allows to store and restore snapshot and uses AWS S3 bucket as storage.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sof2000/python-cassandra-cli",
    install_requires = requirements,
    packages=['python_cassandra_cli'],
    entry_points ={ 
        'console_scripts': [ 
            'python-cassandra-cli = python_cassandra_cli.__main__:cli'
        ] 
    },
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords ='snapshot python package cassandra backup migration',
    python_requires='>=3.6',
)