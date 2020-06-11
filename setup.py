import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="autocelery",
    version="0.1.0",
    author="Arseniy Banayev",
    author_email="arseniy.banayev@gmail.com",
    description="Make celery automatic, like a REPL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="celery reload automatic",
    url="https://github.com/arseniybanayev/autocelery",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing"
    ],
    python_requires=">=3.6",
    
    # Same as in requirements.txt
    install_requires=["celery"]
)