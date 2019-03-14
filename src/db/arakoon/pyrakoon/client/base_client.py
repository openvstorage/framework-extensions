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


class PyrakoonBase(object):
    """
    Arakoon client interface
    """

    def get(self, key, consistency=None):
        # type: (str, Consistency) -> Any
        """
        Retrieves a certain value for a given key
        :param key: The key whose value you are interested in
        :type key: str
        :param consistency: Consistency of the get
        :type consistency: Consistency
        :return: The value associated with the given key
        :rtype: any
        """
        raise NotImplementedError()

    def get_multi(self, keys, must_exist=True):
        # type: (List[str], bool) -> Generator[Tuple[str, any]]
        """
        Get multiple keys at once
        :param keys: All keys to fetch
        :type keys" List[str]
        :param must_exist: Should all listed keys exist
        :type must_exist: bool
        :return: Generator that yields key value pairs
        :rtype: iterable[Tuple[str, any]
        """
        raise NotImplementedError()

    def set(self, key, value, transaction=None):
        # type: (str, any, str) -> None
        """
        Sets the value for a key to a given value
        If the key does not yet have a value associated with it, a new key value pair will be created.
        If the key does have a value associated with it, it is overwritten.
        :param key: The key to set/update
        :type key: str
        :param value: The value to store
        :type value: any
        :param transaction: ID of the transaction to add the update too
        :type transaction: str
        :return: None
        :rtype: NoneType
        """
        raise NotImplementedError()

    def prefix(self, prefix):
        # type: (str) -> Generator[str]
        """
        Lists all keys starting with the given prefix
        :param prefix: Prefix of the key
        :type prefix: str
        :return: Generator that yields keys
        :rtype: iterable[str]
        """
        raise NotImplementedError()

    def prefix_entries(self, prefix):
        # type: (str) -> Generator[Tuple[str, any]]
        """
        Lists all key, value pairs starting with the given prefix
        :param prefix: Prefix of the key
        :type prefix: str
        :return: Generator that yields key, value pairs
        :rtype: iterable[Tuple[str, any]
        """
        raise NotImplementedError()

    def delete(self, key, must_exist=True, transaction=None):
        # type: (str, bool, str) -> any
        """
        Deletes a given key from the store
        :param key; Key to remove
        ;type key: str
        :param must_exist: Should the key exist
        :type must_exist: bool
        :param transaction: Transaction to apply the update too
        :type transaction: id
        :return The previous value in case must_exist=False, None incase must_exist=False
        :rtype: any
        """
        raise NotImplementedError()

    def delete_prefix(self, prefix, transaction=None):
        # type: (str, Optional[str]) -> None
        """
        Removes a given prefix from the store
        :param prefix: Prefix of the key
        :type prefix: str
        :param transaction: Transaction to apply the update too
        :type transaction: str
        :return None
        ;:rtype: NoneType
        """
        raise NotImplementedError()

    def nop(self):
        # type: () -> None
        """
        Executes a nop command
        """
        raise NotImplementedError()

    def exists(self, key):
        # type: (str) -> bool
        """
        Check if key exists
        :param key: Key to check
        :type key: str
        :return True if key exists else False
        :rtype: bool
        """
        raise NotImplementedError()

    def assert_value(self, key, value, transaction=None):
        # type: (str, any, str) -> None
        """
        Asserts a key-value pair
        :param key: Key of the value to assert
        :type key: str
        :param value: Value to assert
        :type value: any
        :param transaction: Transaction to apply the assert too
        :type transaction: str
        :raises: ArakoonAssertionFailed if the value could not be asserted
        :return: None
        :rtype: NoneType
        """
        raise NotImplementedError()

    def assert_exists(self, key, transaction=None):
        # type: (str, str) -> None
        """
        Asserts that a given key exists
        :param key: Key to assert
        :type key: str
        :param transaction: Transaction to apply the assert too
        :type transaction: str
        :raises: ArakoonAssertionFailed if the value could not be asserted
        :return: None
        :rtype: NoneType
        """
        raise NotImplementedError()

    def begin_transaction(self):
        # type: () -> str
        """
        Creates a transaction (wrapper around Arakoon sequences)
        :return: Identifier of the transaction
        :rtype: str
        """
        raise NotImplementedError()

    def apply_transaction(self, transaction, delete=True):
        # type: (str, Optional[bool]) -> None
        """
        Applies a transaction
        :param transaction: Identifier of the transaction
        :type transaction: str
        :param delete: Delete transaction after attempting to apply the transaction
        Disabling this option requires a delete_transaction to be called at some point to avoid memory leaking
        :type delete: bool
        :return: None
        :rtype: NoneType
        """
        raise NotImplementedError()

    def delete_transaction(self, transaction):
        """
        Deletes a transaction
        :param transaction: Identifier of the transaction
        :type transaction: str
        :return: None
        :rtype: NoneType
        """
        raise NotImplementedError()

    @staticmethod
    def _next_prefix(prefix):
        """
        Calculates the next key which is no longer part of the given prefix
        :param prefix: prefix to calculate of
        :type prefix: str
        :return: The next key
        :rtype: str
        """
        array = list(prefix)
        pos = len(array) - 1
        carry = True
        while carry and pos >= 0:
            digit = ord(array[pos]) + 1
            if digit == 256:
                # New digit would go out of the char range
                array[pos] = chr(0)
                pos = pos - 1
            else:
                # New char found which won't be part of the next prefix
                array[pos] = chr(digit)
                carry = False

        if pos >= 0:
            return ''.join(array)
        # Can occur when all the prefix is composed of \xff
        raise ValueError('Prefix {0} has no next'.format(prefix))

    def lock(self, name, wait=None, expiration=60):
        # type: (str, float, float) -> PyrakoonLock
        """
        Returns the Arakoon lock implementation
        :param name: Name to give to the lock
        :type name: str
        :param wait: Wait time for the lock (in seconds)
        :type wait: float
        :param expiration: Expiration time for the lock (in seconds)
        :type expiration: float
        :return: The lock implementation
        :rtype: PyrakoonLock
        """
        raise NotImplementedError()

    def apply_callback_transaction(self, transaction_callback, max_retries=0, retry_wait_function=None):
        # type: (callable, int, callable) -> None
        """
        Apply a transaction which is the result of the callback.
        The callback should build the complete transaction again to handle the asserts. If the possible previous run was interrupted,
        the Arakoon might only have partially applied all actions therefore all asserts must be re-evaluated
        Handles all Arakoon errors by re-executing the callback until it finished or until no more retries can be made
        :param transaction_callback: Callback function which returns the transaction ID to apply
        :type transaction_callback: callable
        :param max_retries: Number of retries to try. Retries are attempted when an AssertException is thrown.
        Defaults to 0
        :param retry_wait_function: Function called retrying the transaction. The current try number is passed as an argument
        Defaults to lambda retry: time.sleep(randint(0, 25) / 100.0)
        :type retry_wait_function: callable
        :return: None
        :rtype: NoneType
        """
        raise NotImplementedError()
