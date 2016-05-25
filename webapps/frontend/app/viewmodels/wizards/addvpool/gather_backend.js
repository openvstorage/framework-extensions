// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define */
define([
    'require', 'jquery', 'knockout',
    'ovs/api', 'ovs/generic',
    './data'
], function(require, $, ko, api, generic, data) {
    "use strict";
    return function() {
        var self = this;
        try {
            require(['../../containers/albabackend'], function(AlbaBackend){
                self.AlbaBackend = AlbaBackend;
            })
        }
        catch(err) {
            console.log('Alba not installed');
            self.AlbaBackend = undefined;
        }
        // Variables
        self.data = data;

        // Handles
        self.fetchAlbaVPoolHandle = undefined;

        // Observables
        self.albaBackendLoading = ko.observable(false);
        self.albaPresetMap      = ko.observable({});
        self.invalidAlbaInfo    = ko.observable(false);

        // Computed
        self.localBackendsAvailable = ko.computed(function() {
            if (self.data.localHost() && self.data.albaBackends().length < 2) {
                if (self.data.editBackend()) {
                    self.data.aaLocalHost(false);
                }
                return false;
            }
            if (self.data.editBackend()) {
                self.data.aaLocalHost(true);
            }
            return true;
        });
        self.isPresetAvailable = ko.computed(function() {
            var presetAvailable = true;
            if (self.data.albaAABackend() !== undefined && self.data.albaAAPreset() !== undefined) {
                var guid = self.data.albaAABackend().guid(),
                    name = self.data.albaAAPreset().name;
                if (self.albaPresetMap().hasOwnProperty(guid) && self.albaPresetMap()[guid].hasOwnProperty(name)) {
                    presetAvailable = self.albaPresetMap()[guid][name];
                }
            }
            return presetAvailable;
        });
        self.reUseableStorageRouters = ko.computed(function() {
            var temp = self.data.storageRoutersUsed().slice();  // Make deep copy of the list
            temp.unshift(undefined);  // Insert undefined as element 0
            return temp;
        });
        self.canContinue = ko.computed(function() {
            var valid = true, showErrors = false, reasons = [], fields = [];
            if (!self.data.useAA()) {
                return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
            }
            if (self.data.backend() === 'alba') {
                if (self.data.albaAABackend() === undefined) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.add_vpool.gather_backend.choose_backend'));
                    fields.push('backend');
                }
                if (!self.data.aaLocalHost()) {
                    if (!self.data.aaHost.valid()) {
                        valid = false;
                        fields.push('host');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.invalid_host'));
                    }
                    if (self.data.aaAccesskey() === '' || self.data.aaSecretkey() === '') {
                        valid = false;
                        fields.push('clientid');
                        fields.push('clientsecret');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.no_credentials'));
                    }
                    if (self.invalidAlbaInfo()) {
                        valid = false;
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.invalid_alba_info'));
                        fields.push('clientid');
                        fields.push('clientsecret');
                        fields.push('host');
                    }
                }
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });

        // Functions
        self.loadAlbaBackends = function() {
            return $.Deferred(function(albaDeferred) {
                generic.xhrAbort(self.fetchAlbaVPoolHandle);
                var relay = '', remoteInfo = {},
                    getData = {
                        backend_type: 'alba',
                        contents: '_dynamics'
                    };
                if (!self.data.aaLocalHost()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.aaHost();
                    remoteInfo.port = self.data.aaPort();
                    remoteInfo.client_id = self.data.aaAccesskey().replace(/\s+/, "");
                    remoteInfo.client_secret = self.data.aaSecretkey().replace(/\s+/, "");
                }
                $.extend(getData, remoteInfo);
                self.albaBackendLoading(true);
                self.invalidAlbaInfo(false);
                self.fetchAlbaVPoolHandle = api.get(relay + 'backends', { queryparams: getData })
                    .done(function(data) {
                        var available_backends = [], calls = [];
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.linked_guid + '/', { queryparams: getData })
                                        .then(function(data) {
                                            if (data.available === true && data.guid !== self.data.albaBackend().guid()) {
                                                var asdsFound = false;
                                                $.each(data.asd_statistics, function(key, value) {  // As soon as we enter loop, we know at least 1 ASD is linked to this backend
                                                    asdsFound = true;
                                                    return false;
                                                });
                                                if (asdsFound === true) {
                                                    available_backends.push(data);
                                                    self.albaPresetMap()[data.guid] = {};
                                                    $.each(data.presets, function (_, preset) {
                                                        self.albaPresetMap()[data.guid][preset.name] = preset.is_available;
                                                    });
                                                }
                                            }
                                        })
                                );
                            }
                        });
                        $.when.apply($, calls)
                            .then(function() {
                                if (available_backends.length > 0) {
                                    var guids = [], abData = {};
                                    $.each(available_backends, function(index, item) {
                                        guids.push(item.guid);
                                        abData[item.guid] = item;
                                    });
                                    generic.crossFiller(
                                        guids, self.data.albaAABackends,
                                        function(guid) {
                                            return new self.AlbaBackend(guid);
                                        }, 'guid'
                                    );
                                    $.each(self.data.albaAABackends(), function(index, albaBackend) {
                                        albaBackend.fillData(abData[albaBackend.guid()]);
                                    });
                                    self.data.albaAABackends.sort(function(backend1, backend2) {
                                        return backend1.name() < backend2.name() ? -1 : 1;
                                    });
                                    self.data.albaAABackend(self.data.albaAABackends()[0]);
                                    self.data.albaAAPreset(self.data.albaAABackends()[0].enhancedPresets()[0]);
                                } else {
                                    self.data.albaAABackends([]);
                                    self.data.albaAABackend(undefined);
                                    self.data.albaAAPreset(undefined);
                                }
                                self.albaBackendLoading(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                self.data.albaAABackends([]);
                                self.data.albaAABackend(undefined);
                                self.data.albaAAPreset(undefined);
                                self.albaBackendLoading(false);
                                self.invalidAlbaInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function() {
                        self.data.albaAABackends([]);
                        self.data.albaAABackend(undefined);
                        self.data.albaAAPreset(undefined);
                        self.albaBackendLoading(false);
                        self.invalidAlbaInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            if (self.data.albaBackend() !== undefined && self.data.albaAABackend() !== undefined && self.data.albaBackend().guid() === self.data.albaAABackend().guid()) {
                self.data.albaAABackends([]);
                $.each(self.data.albaBackends(), function (_, backend) {
                    if (backend !== self.data.albaBackend() && !self.data.albaAABackends().contains(backend)) {
                        self.data.albaAABackends().push(backend);
                    }
                });
                if (self.data.albaAABackends().length === 0) {
                    self.data.albaAABackend(undefined);
                    self.data.albaAAPreset(undefined);
                } else {
                    self.data.albaAABackend(self.data.albaAABackends()[0]);
                    self.data.albaAAPreset(self.data.albaAABackends()[0].enhancedPresets()[0]);
                }
            }
        };
    };
});

