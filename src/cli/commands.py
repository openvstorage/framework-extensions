# Copyright (C) 2019 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

import os
import click
import subprocess
from ovs_extensions.constants.scripts import BASE_SCRIPTS


class OVSHelpFormatter(click.HelpFormatter):
    """
    Custom Helpformatter to write out a similar output as before
    """

    def __init__(self, indent_increment=2, width=float('inf'), max_width=float('inf')):
        super(OVSHelpFormatter, self).__init__(indent_increment=indent_increment, width=width, max_width=max_width)

    @staticmethod
    def get_formatted_row(command, command_chain=''):
        # type: (click.Command, str) -> Tuple[str, str]
        """
        :param command: Command to generate row for
        :param command_chain: FUll path to the command (if relevant)
        :return: Tuple representing two columns of the row
        :rtype: Tuple[str, str]
        """
        additional_info = ''
        if not command_chain:
            command_chain = command.name
        command_chain = '{0} {1}'.format(command_chain.strip(), command.name)
        if isinstance(command, OVSCommand):
            additional_info = command.command_parameter_help
        return '- {0} {1}'.format(command_chain, additional_info), command.help

    @staticmethod
    def get_formatted_section_header(command):
        # type: (click.Command) -> str
        """
        Format a section header for the command
        :param command: Command to format a header for
        :type command: click.Command
        :return: The section header
        :rtype: str
        """
        if isinstance(command, OVSCommand) and command.section_header:
            name = command.section_header
        else:
            name = command.name
        return '* {0} options'.format(name.title())


class OVSBaseGroup(click.Group):

    def get_help(self, ctx):
        """
        Formats the help into a string and returns it.  This creates a
        formatter and will call into the following formatting methods:
        Override uses a different formatter
        """
        formatter = OVSHelpFormatter()
        self.format_help(ctx, formatter)
        return formatter.getvalue().rstrip('\n')

    def get_format_commands(self, command_chain=''):
        # type: (str) -> Tuple[str, List[str]]
        """
        Get all formatted commands
        :return: The section name, all command rows
        """
        rows = []
        command_chain = command_chain.strip()
        commands = self.commands.values()
        if self.callback:
            commands = [self] + commands
        for command in commands:  # type: Union[click.Command, OVSCommand]
            if isinstance(command, OVSBaseGroup) and command != self:
                # Does recursion and adds it under the parent section
                sub_section_header, sub_rows = command.get_format_commands(command_chain)
                rows.extend(sub_rows)
            else:
                if command == self:
                    chain = command_chain
                else:
                    chain = '{0} {1}'.format(command_chain, self.name)
                rows.append(OVSHelpFormatter.get_formatted_row(command, chain))
        section_header = OVSHelpFormatter.get_formatted_section_header(self)
        return section_header, rows


class OVSCLI(OVSBaseGroup):
    """
    Click CLI which will dynamically loads all addon commands
    Implementations require an entry point
    An entry point is defined as:
    @click.group(cls=CLI)
    def entry_point():
        pass
    if __name__ == '__main__':
        entry_point()
    """

    def __init__(self, *args, **kwargs):
        # type: (*any, **any) -> None
        super(OVSCLI, self).__init__(*args, **kwargs)

    def list_commands(self, ctx):
        # type: (click.Context) ->List[str]
        """
        Lists all possible commands found within the directory of this file
        All modules are retrieved
        :param ctx: Passed context
        :return: List of files to look for commands
        :rtype: List[str]
        """
        _ = ctx
        non_dynamic = self.commands.keys()
        sub_commands = self._discover_methods().keys()  # Returns all underlying modules
        total_commands = non_dynamic + sub_commands
        total_commands.sort()
        return total_commands

    def get_command(self, ctx, name):
        # type: (click.Context, str) -> callable
        """
        Retrieves a command to execute
        :param ctx: Passed context
        :param name: Name of the command
        :return: Function pointer to the command or None when no import could happen
        :rtype: callable
        """
        cmd = self.commands.get(name)
        if cmd:
            return cmd
        # More extensive - build the command and register
        discovery_data = self._discover_methods()
        if name in discovery_data.keys():
            script_path = discovery_data[name]
            # The current passed name is a module. Wrap it up in a group and add all commands under it dynamically
            command = AddonCommand(script_path=script_path, name=name)
            self.add_command(command)
            return command

    @classmethod
    def _discover_methods(cls):
        # type: () -> Dict[str, str]
        """
        Discovers all possible scripts within the BASE_SCRIPTS folder
        :return: Dict with the filename as key and the path as value
        :rtype: Dict[str, str]
        """
        discovered = {}
        for name in os.listdir(BASE_SCRIPTS):
            full_path = os.path.join(BASE_SCRIPTS, name)
            if os.path.isfile(full_path) and name.endswith('.sh'):
                discovered[name.rstrip('.sh')] = full_path
        return discovered

    def format_commands(self, ctx, formatter):
        # type: (click.Context, click.HelpFormatter) -> None
        """
        Extra format methods for multi methods that adds all the commands after the options.
        Overruled to add Addon commands as a separate list and to give the full overview as before
        """
        commands = []
        addon_commands = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            # What is this, the tool lied about a command. Ignore it
            if cmd is None:
                continue
            if isinstance(cmd, AddonCommand):
                addon_commands.append(cmd)
            else:
                commands.append(cmd)

        for command_section, command_list in [('Commands', commands), ('Addons', addon_commands)]:
            if command_list:
                with formatter.section(command_section):
                    rows = []
                    headers_commands = {}
                    for cmd in command_list:
                        help = cmd.short_help or ''
                        if isinstance(cmd, OVSBaseGroup):
                            section, command_rows = cmd.get_format_commands(command_chain=self.name)
                            with formatter.section(section):
                                formatter.write_dl(command_rows)
                        elif isinstance(cmd, OVSCommand):
                            if cmd.section_header:
                                if cmd.section_header in headers_commands:
                                    headers_commands[cmd.section_header].append(cmd)
                                else:
                                    headers_commands[cmd.section_header] = [cmd]
                            else:
                                rows.append((cmd.name, help))
                        else:
                            rows.append((cmd.name, help))
                    for header, commands in headers_commands.iteritems():
                        with formatter.section(OVSHelpFormatter.get_formatted_section_header(commands[0])):
                            formatter.write_dl([OVSHelpFormatter.get_formatted_row(i, command_chain=self.name) for i in commands])
                    if rows:
                        formatter.write_dl(rows)

    def main(self, args=None, prog_name=None, complete_var=None, standalone_mode=True, **extra):
        """
        Main entry point into a command
        Changed to inject the program name as it will otherwise set the sys.arg[0] which is entry.py
        """
        prog_name = prog_name or 'ovs'
        super(OVSCLI, self).main(args, prog_name, complete_var, standalone_mode, **extra)


class OVSGroup(OVSBaseGroup):
    """
    OVS Command group.
    This class is used for pretty printing the help
    """

    def __init__(self, *args, **kwargs):
        super(OVSGroup, self).__init__(*args, **kwargs)


class OVSCommand(click.Command):

    def __init__(self, name, command_parameter_help='', section_header='', *args, **kwargs):
        """
        :param name: Name of the command
        :param command_parameter_help: Extra help to print when displaying the commands in a list
        """
        self.command_parameter_help = command_parameter_help
        self.section_header = section_header
        super(OVSCommand, self).__init__(name, *args, **kwargs)


class AddonCommand(click.Command):
    """
    Command representing an addon entry
    An addon entry is a script placed by framework addons that can be invoked through ovs <addon>
    """
    def __init__(self, script_path, name, *args, **kwargs):
        """
        Initialize an addon script
        :param script_path: Path to the addon script
        :type script_path: str
        :param name: Name of the addon script
        :type name: str
        """
        self.script_path = script_path
        help = 'Run the command for the {} addon'.format(name)
        super(AddonCommand, self).__init__(callback=self.script_callback, name=name, help=help, *args, **kwargs)

    def parse_args(self, ctx, args):
        # type: (click.Context, list) -> list
        """
        Override the parse_args. We only care about the string values given to the addon command
        It'll decide what to do with the arguments itself
        This also disables click retrieving the help. Itll pass it along to the addon
        :param ctx: Context
        :type ctx: click.Context
        :param args: Supplied args
        :type args: list
        :return: List of args
        :rtype: list
        """
        ctx.args = args
        return args

    def script_callback(self, *args, **kwargs):
        # type: (*any, **any) -> None
        """
        Callback that invokes the passed on script
        :return: None
        """
        _ = kwargs
        print subprocess.check_output([self.script_path] + list(args))

    def invoke(self, ctx):
        # type: (click.Context) -> any
        """
        Overrule the invoke. This click command does not process any arguments and just passes them on
        :param ctx: Context given
        :type ctx: click.Context
        :return: Output of the addon command
        :rtype: any
        """
        if self.callback is not None:
            return ctx.invoke(self.callback, *ctx.args)
