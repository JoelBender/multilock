from setuptools import setup, find_packages

setup(
    name="multilock",
    version="1.0",
    description="Multilock",
    packages=find_packages(),
    install_requires=["redis"],
)
