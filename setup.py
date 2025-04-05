from setuptools import setup, find_packages

setup(
    name="koladapy",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.1.0",
        "tqdm>=4.50.0",
        "backoff>=1.10.0",
    ],
    python_requires=">=3.7",
    author="Emanuel Raptis",
    description="A Python wrapper for the Kolada API v.3 (Swedish municipalities data)",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/xemarap/koladapy",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Analysts",
        "Topic :: Data Analysis :: Information Analysis :: Kolada",
    ]
)