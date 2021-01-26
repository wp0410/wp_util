import setuptools

with open("README.md", "r", encoding="utf-8") as fh_readme:
    long_description = fh_readme.read()

setuptools.setup(
    name = "wp_util",
    version = "0.0.dev11",
    author = "Walter Pachlinger",
    author_email = "walter.pachlinger@gmail.com",
    description = "A collection of useful utilities",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "",
    packages = setuptools.find_packages(),
    classifiers = [
        "Programming Language :: Python :: 3",
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: European Union Public Licence 1.2 (EUPL 1.2)",
        "Operating System :: OS Independent"
    ],
    python_requires = ">= 3.7"
)

