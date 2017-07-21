from distutils.core import setup

setup(
    name='failhadoop',
    version='0.1.0',
    packages=['failhadoop'],
    scripts=['bin/fail.py', 'bin/get_diff.py', 'bin/restart_services.py', 'bin/update_configs.py'],
    data_files=[
                ('conf', ['conf/ansible.cfg','conf/sbathe.json','conf/ss.json']),
                ('docs', ['conf/postdata.json'])
               ],
    url='https://github.com/sbathehwx/failhadoop',
    license='Apache Licence 2.0',
    author='Saurabh Bathe',
    author_email='sbathe@hortonworks.com',
    description='A framework for running various failure tests against Ambari managed Hadoop cluster(s)',
    install_requires=[
        "requests >= 2.13.0",
        "ambariclient >= 0.5.10"
    ]
)
