import click
from ovs_extensions.generic.unittests import UnitTest


@click.command('unittest', help='Run all or a part of the OVS unittest suite')
@click.argument('action', required=False, default=None, type=click.STRING)
@click.option('--averages', is_flag=True, default=False)
def unittest(action, averages):
    from ovs_extensions.storage.volatilefactory import VolatileFactory
    from ovs_extensions.storage.persistentfactory import PersistentFactory
    from ovs_extensions.services.servicefactory import ServiceFactory

    # Clear the stores. It will force to recompute
    VolatileFactory.store = None
    PersistentFactory.store = None
    ServiceFactory.manager = None

    ut = UnitTest('ovs')
    if not action:
        ut.run_tests(add_averages=averages)
    elif action == 'list':
        ut.list_tests(print_tests=True)
    else:
        action = str(action)

        if action.endswith('.py'):
            filename = action.rstrip('.py')
        else:
            filename = action
        ut.run_tests(filename, add_averages=averages)