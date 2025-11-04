from setuptools import setup, find_packages

setup(
    name="ControlDB",
    version="1.2.3",
    author="Vito Pol",
    author_email="don_vito_pol@hotmail.com",
    description=(
        "ControlDB: Utilities for database management, "
        "processing sample replicates, and logging."
    ),
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/ControlDB",  # optional but good practice
    packages=find_packages(exclude=["tests*", "examples*"]),
    python_requires=">=3.11",
    install_requires=[
        "pandas>=2.0",
        "pyodbc>=4.0",
        "SQLAlchemy>=2.0",
        "pretty_logger>=0.1",  # replace with exact version if pinned
    ],
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Database :: Front-Ends",
    ],
)