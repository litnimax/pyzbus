from setuptools import setup


setup(
    name='pyzbus',
    packages=['pyzbus'],
    version='0.1',
    description='Python ZeroMQ Pub/Sub bus',
    author='litnimax',
    author_email='litnimax@pbxware.ru',
    url='https://github.com/litnimax/pyzbus',
    license='BSD',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Topic :: Software Development :: Libraries',
    ],
    install_requires=[
        'gevent',
        'zmq',
    ]
)
