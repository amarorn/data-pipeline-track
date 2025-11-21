"""
Setup para Track Platform - Bibliotecas compartilhadas
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="track-platform",
    version="1.0.0",
    author="Track Data Team",
    author_email="data-team@track.com",
    description="Bibliotecas compartilhadas para pipelines de dados Track",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.11",
    install_requires=[
        "pyspark>=3.4.1",
        "pandas>=2.0.3",
        "cx-Oracle>=8.3.0",
        "clickhouse-driver>=0.2.6",
        "clickhouse-connect>=0.6.14",
        "boto3>=1.28.57",
        "s3fs>=2023.9.2",
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0.1",
        "loguru>=0.7.2",
    ],
)
