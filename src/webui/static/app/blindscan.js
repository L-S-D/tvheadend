/*
 * Blindscan UI for DVB-S/S2 networks
 *
 * Provides spectrum acquisition, peak detection, and automatic mux creation
 * for satellites using Neumo DVB driver extensions.
 */

tvheadend.blindscan = {
    session: null,
    window: null,
    canvas: null,
    spectrumData: [],
    peaks: [],
    scanPol: 'B',  // Track which polarisation was selected
    starting: false,  // Prevent double-start

    /**
     * Open blindscan modal for a network
     */
    openModal: function(network_uuid, network_name) {
        var self = this;

        if (this.window) {
            this.window.close();
        }

        // Satconf store - linked to network
        var satconfStore = new Ext.data.JsonStore({
            url: 'api/blindscan',
            baseParams: { 'op': 'list_satconfs', 'network_uuid': network_uuid },
            root: 'entries',
            fields: ['frontend_uuid', 'frontend_name', 'satconf_uuid', 'satconf_name',
                     'lnb_type', 'unicable', 'unicable_type', 'scr', 'scr_freq', 'display_name'],
            autoLoad: true,
            listeners: {
                load: function(store) {
                    // Auto-select: prefer fe0/scr8, else first satconf
                    if (store.getCount() > 0) {
                        var preferred = null;
                        store.each(function(rec) {
                            // Look for adapter 0 with SCR 8
                            if (rec.data.frontend_name && rec.data.frontend_name.indexOf('#0') >= 0 &&
                                rec.data.scr === 8) {
                                preferred = rec;
                                return false; // stop iteration
                            }
                        });
                        if (!preferred) {
                            preferred = store.getAt(0);
                        }
                        satconfSelect.setValue(preferred.data.satconf_uuid);
                    }
                }
            }
        });

        // Combined satconf selector (shows frontend + satconf + LNB info)
        var satconfSelect = new Ext.form.ComboBox({
            fieldLabel: _('Satconf'),
            id: 'blindscan-satconf',
            store: satconfStore,
            mode: 'local',
            displayField: 'display_name',
            valueField: 'satconf_uuid',
            triggerAction: 'all',
            editable: false,
            width: 500
        });

        // Helper to get frontend UUID from selected satconf
        function getSelectedFrontendUuid() {
            var val = satconfSelect.getValue();
            var idx = satconfStore.find('satconf_uuid', val);
            if (idx >= 0) {
                return satconfStore.getAt(idx).data.frontend_uuid;
            }
            return '';
        }

        // Helper to check if selected satconf is unicable
        function isSelectedUnicable() {
            var val = satconfSelect.getValue();
            var idx = satconfStore.find('satconf_uuid', val);
            if (idx >= 0) {
                return satconfStore.getAt(idx).data.unicable;
            }
            return false;
        }

        // Create spectrum canvas
        var canvasPanel = new Ext.BoxComponent({
            autoEl: {
                tag: 'canvas',
                width: 800,
                height: 200
            },
            listeners: {
                afterrender: function(comp) {
                    self.canvas = comp.el.dom;
                    self.drawSpectrum();
                }
            }
        });

        // Progress bar
        var progressBar = new Ext.ProgressBar({
            id: 'blindscan-progress',
            text: _('Ready'),
            width: 780
        });

        // Status text
        var statusText = new Ext.form.DisplayField({
            id: 'blindscan-status',
            value: _('Select frontend and satconf, then click Start'),
            width: 780
        });

        // Peak count display
        var peakCount = new Ext.form.DisplayField({
            id: 'blindscan-peaks',
            fieldLabel: _('Detected Peaks'),
            value: '0',
            width: 100
        });

        var muxCount = new Ext.form.DisplayField({
            id: 'blindscan-muxes',
            fieldLabel: _('Created Muxes'),
            value: '0',
            width: 100
        });

        // Filter checkbox to hide existing muxes
        var hideExistingCheck = new Ext.form.Checkbox({
            id: 'blindscan-hide-existing',
            boxLabel: _('Hide existing'),
            checked: false,
            listeners: {
                check: function(cb, checked) {
                    if (self.peaksStore) {
                        if (checked) {
                            self.peaksStore.filterBy(function(rec) {
                                return !rec.get('existing') && rec.get('status') !== 'existing';
                            });
                        } else {
                            self.peaksStore.clearFilter();
                        }
                    }
                }
            }
        });

        // Frequency range inputs
        var startFreq = new Ext.form.NumberField({
            fieldLabel: _('Start (MHz)'),
            id: 'blindscan-start-freq',
            value: 10700,
            minValue: 950,
            maxValue: 12750,
            width: 70
        });

        var endFreq = new Ext.form.NumberField({
            fieldLabel: _('End (MHz)'),
            id: 'blindscan-end-freq',
            value: 12750,
            minValue: 950,
            maxValue: 12750,
            width: 70
        });

        // Polarisation selector
        var polSelect = new Ext.form.ComboBox({
            fieldLabel: _('Polarisation'),
            id: 'blindscan-pol',
            store: new Ext.data.SimpleStore({
                fields: ['value', 'text'],
                data: [
                    ['B', _('Both')],
                    ['H', _('Horizontal')],
                    ['V', _('Vertical')]
                ]
            }),
            mode: 'local',
            displayField: 'text',
            valueField: 'value',
            value: 'B',
            triggerAction: 'all',
            editable: false,
            width: 90
        });

        // Resolution (kHz)
        var resolutionSelect = new Ext.form.ComboBox({
            fieldLabel: _('Resolution'),
            id: 'blindscan-resolution',
            store: new Ext.data.SimpleStore({
                fields: ['value', 'text'],
                data: [
                    [0, _('Auto')],
                    [25, '25 kHz'],
                    [50, '50 kHz'],
                    [100, '100 kHz'],
                    [250, '250 kHz'],
                    [500, '500 kHz'],
                    [1000, '1 MHz'],
                    [2000, '2 MHz']
                ]
            }),
            mode: 'local',
            displayField: 'text',
            valueField: 'value',
            value: 0,
            triggerAction: 'all',
            editable: false,
            width: 80
        });

        // FFT Size
        var fftSizeSelect = new Ext.form.ComboBox({
            fieldLabel: _('FFT Size'),
            id: 'blindscan-fft-size',
            store: new Ext.data.SimpleStore({
                fields: ['value', 'text'],
                data: [
                    [256, '256'],
                    [512, '512 (default)'],
                    [1024, '1024'],
                    [2048, '2048']
                ]
            }),
            mode: 'local',
            displayField: 'text',
            valueField: 'value',
            value: 512,
            triggerAction: 'all',
            editable: false,
            width: 100
        });

        // Peak Detection Method
        var peakDetectSelect = new Ext.form.ComboBox({
            fieldLabel: _('Peak Detection'),
            id: 'blindscan-peak-detect',
            store: new Ext.data.SimpleStore({
                fields: ['value', 'text'],
                data: [
                    [0, 'Auto (driver + fallback)'],
                    [1, 'Driver only'],
                    [2, 'Algorithm only']
                ]
            }),
            mode: 'local',
            displayField: 'text',
            valueField: 'value',
            value: 0,
            triggerAction: 'all',
            editable: false,
            width: 140
        });

        // Start button
        var startButton = new Ext.Button({
            text: _('Start'),
            iconCls: 'find',
            handler: function() {
                var sc = satconfSelect.getValue();
                var fe = getSelectedFrontendUuid();
                if (!sc || !fe) {
                    Ext.Msg.alert(_('Error'), _('Please select a satconf'));
                    return;
                }
                self.startScan(
                    network_uuid,
                    fe,
                    sc,
                    startFreq.getValue() * 1000,  // MHz to kHz
                    endFreq.getValue() * 1000,
                    polSelect.getValue(),
                    resolutionSelect.getValue() || 0,
                    fftSizeSelect.getValue(),
                    peakDetectSelect.getValue(),
                    isSelectedUnicable()
                );
            }
        });

        // Cancel button
        var cancelButton = new Ext.Button({
            text: _('Cancel'),
            iconCls: 'cancel',
            disabled: true,
            id: 'blindscan-cancel-btn',
            handler: function() {
                self.cancelScan();
            }
        });

        // Peaks grid store
        var peaksStore = new Ext.data.JsonStore({
            fields: [
                { name: 'selected', type: 'boolean' },
                { name: 'frequency', type: 'int' },
                { name: 'symbol_rate', type: 'int' },
                { name: 'level', type: 'int' },
                { name: 'snr', type: 'int' },
                { name: 'polarisation', type: 'string' },
                { name: 'status', type: 'string' },
                { name: 'modulation', type: 'string' },
                { name: 'fec', type: 'string' },
                { name: 'rolloff', type: 'string' },
                { name: 'pilot', type: 'string' },
                { name: 'pls_mode', type: 'string' },
                { name: 'pls_code', type: 'int' },
                { name: 'stream_id', type: 'int' },
                { name: 'delsys', type: 'string' },
                { name: 'multistream', type: 'boolean' },
                { name: 'isi_list' },  // Array of ISIs
                { name: 'existing', type: 'boolean' },
                { name: 'is_gse', type: 'boolean' },
                { name: 'verified_freq', type: 'int' },
                { name: 'verified_sr', type: 'int' }
            ]
        });
        this.peaksStore = peaksStore;

        // Checkbox selection model (single instance shared with columns)
        var peaksSM = new Ext.grid.CheckboxSelectionModel({
            singleSelect: false
        });

        // Peaks grid with checkbox selection and prescan button
        var peaksGrid = new Ext.grid.GridPanel({
            store: peaksStore,
            id: 'blindscan-peaks-grid',
            sm: peaksSM,
            columns: [
                peaksSM,  // Include selection model in columns
                {
                    header: _('Freq (MHz)'),
                    dataIndex: 'frequency',
                    width: 90,
                    renderer: function(v, meta, rec) {
                        var vf = rec.get('verified_freq');
                        if (vf > 0) {
                            return '<span style="color:#08f" title="Verified: ' + (vf / 1000).toFixed(2) + ' MHz">' +
                                   (vf / 1000).toFixed(2) + '</span>';
                        }
                        return (v / 1000).toFixed(2);
                    }
                },
                {
                    header: _('SR (ksps)'),
                    dataIndex: 'symbol_rate',
                    width: 80,
                    renderer: function(v, meta, rec) {
                        var vs = rec.get('verified_sr');
                        if (vs > 0) {
                            return '<span style="color:#08f" title="Verified: ' + (vs / 1000).toFixed(0) + ' ksps">' +
                                   (vs / 1000).toFixed(0) + '</span>';
                        }
                        return v ? (v / 1000).toFixed(0) : '?';
                    }
                },
                {
                    header: _('Level'),
                    dataIndex: 'level',
                    width: 55,
                    renderer: function(v) { return (v / 100).toFixed(1) + ' dB'; }
                },
                {
                    header: _('Pol'),
                    dataIndex: 'polarisation',
                    width: 35
                },
                {
                    header: _('Sys'),
                    dataIndex: 'delsys',
                    width: 55
                },
                {
                    header: _('Mod'),
                    dataIndex: 'modulation',
                    width: 60
                },
                {
                    header: _('FEC'),
                    dataIndex: 'fec',
                    width: 45
                },
                {
                    header: _('ISI'),
                    dataIndex: 'stream_id',
                    width: 35,
                    renderer: function(v) {
                        if (v === undefined || v < 0) return '';
                        return v;
                    }
                },
                {
                    header: _('PLS'),
                    dataIndex: 'pls_code',
                    width: 50,
                    renderer: function(v, meta, rec) {
                        var mode = rec.get('pls_mode');
                        if (!mode) return '';
                        return mode + ':' + v;
                    }
                },
                {
                    header: _('Status'),
                    dataIndex: 'status',
                    width: 70,
                    renderer: function(v) {
                        var colors = {
                            'pending': '#888',
                            'scanning': '#ff0',
                            'locked': '#0f0',
                            'failed': '#f00',
                            'existing': '#08f',
                            'retry': '#f80'
                        };
                        return '<span style="color:' + (colors[v] || '#fff') + '">' + v + '</span>';
                    }
                },
                {
                    header: '',
                    width: 60,
                    dataIndex: 'status',
                    renderer: function(v, meta, rec, rowIndex) {
                        var freq = rec.get('frequency');
                        return '<a href="#" onclick="tvheadend.blindscan.prescanPeak(' +
                               rowIndex + '); return false;" ' +
                               'style="color:#0066cc;text-decoration:underline;">' +
                               _('Scan') + '</a>';
                    }
                }
            ],
            stripeRows: true,
            autoExpandColumn: false,
            height: 150
        });

        // Create muxes button
        var createMuxesButton = new Ext.Button({
            text: _('Create Muxes'),
            iconCls: 'add',
            disabled: true,
            id: 'blindscan-create-btn',
            handler: function() {
                self.createMuxes();
            }
        });


        // Create window
        this.window = new Ext.Window({
            title: _('Blindscan') + ' - ' + network_name,
            width: 900,
            height: 720,
            layout: 'vbox',
            layoutConfig: { align: 'stretch' },
            modal: true,
            items: [
                {
                    xtype: 'panel',
                    layout: 'hbox',
                    border: false,
                    padding: 10,
                    height: 35,
                    items: [
                        satconfSelect
                    ]
                },
                {
                    xtype: 'panel',
                    layout: 'hbox',
                    border: false,
                    padding: '0 10 10 10',
                    height: 35,
                    items: [
                        startFreq,
                        { xtype: 'spacer', width: 10 },
                        endFreq,
                        { xtype: 'spacer', width: 10 },
                        polSelect,
                        { xtype: 'spacer', width: 10 },
                        resolutionSelect,
                        { xtype: 'spacer', width: 10 },
                        fftSizeSelect,
                        { xtype: 'spacer', width: 10 },
                        peakDetectSelect,
                        { xtype: 'spacer', width: 20 },
                        startButton,
                        { xtype: 'spacer', width: 10 },
                        cancelButton
                    ]
                },
                {
                    xtype: 'panel',
                    padding: '5 10',
                    border: false,
                    height: 30,
                    items: [progressBar]
                },
                {
                    xtype: 'panel',
                    padding: '5 10',
                    border: false,
                    height: 25,
                    items: [statusText]
                },
                {
                    xtype: 'panel',
                    height: 210,
                    layout: 'fit',
                    padding: '5 10',
                    items: [canvasPanel]
                },
                {
                    xtype: 'panel',
                    flex: 1,
                    layout: 'fit',
                    padding: '0 10',
                    items: [peaksGrid]
                },
                {
                    xtype: 'panel',
                    layout: 'hbox',
                    border: false,
                    padding: 10,
                    height: 35,
                    items: [
                        peakCount,
                        { xtype: 'spacer', width: 40 },
                        muxCount,
                        { xtype: 'spacer', width: 40 },
                        hideExistingCheck,
                        { xtype: 'spacer', flex: 1 },
                        createMuxesButton
                    ]
                }
            ],
            buttons: [{
                text: _('Close'),
                handler: function() {
                    self.window.close();
                }
            }],
            listeners: {
                close: function() {
                    // Release the session (closes frontend fd)
                    var uuid = self.session || self.lastSession;
                    if (uuid) {
                        tvheadend.Ajax({
                            url: 'api/blindscan',
                            params: { op: 'release', uuid: uuid }
                        });
                    }
                    self.session = null;
                    self.lastSession = null;
                    self.window = null;
                    self.canvas = null;
                    self.peaksStore = null;
                    self.starting = false;
                }
            }
        });

        this.window.show();
    },

    /**
     * Start blindscan
     */
    startScan: function(network_uuid, frontend_uuid, satconf_uuid, start_freq, end_freq, pol, resolution, fft_size, peak_detect, is_unicable) {
        var self = this;

        // Prevent double-click (set flag BEFORE async call)
        if (this.starting) {
            return;
        }
        this.starting = true;

        this.spectrumData = [];
        this.peaks = [];
        this.scanPol = pol;  // Remember the polarisation for spectrum fetch
        this.drawSpectrum();

        Ext.getCmp('blindscan-cancel-btn').enable();
        Ext.getCmp('blindscan-progress').updateProgress(0, _('Starting...'));

        tvheadend.Ajax({
            url: 'api/blindscan',
            params: {
                op: 'start',
                network_uuid: network_uuid,
                frontend_uuid: frontend_uuid,
                satconf_uuid: satconf_uuid || '',
                start_freq: start_freq,
                end_freq: end_freq,
                polarisation: pol,
                resolution: resolution,
                fft_size: fft_size,
                peak_detect: peak_detect,
                unicable: is_unicable ? 1 : 0
            },
            success: function(d) {
                var data = Ext.decode(d.responseText);
                if (data.uuid) {
                    self.session = data.uuid;
                    self.pollStatus();
                } else {
                    self.starting = false;
                    Ext.getCmp('blindscan-status').setValue(data.error || _('Failed to start'));
                    Ext.getCmp('blindscan-cancel-btn').disable();
                }
            },
            failure: function(response) {
                self.starting = false;
                Ext.getCmp('blindscan-cancel-btn').disable();
            }
        });
    },

    /**
     * Poll session status
     */
    pollStatus: function() {
        var self = this;

        if (!this.session) return;

        tvheadend.Ajax({
            url: 'api/blindscan',
            params: {
                op: 'status',
                uuid: this.session
            },
            success: function(d) {
                var data = Ext.decode(d.responseText);

                // Update progress
                var progress = data.progress || 0;
                var progressBar = Ext.getCmp('blindscan-progress');
                progressBar.updateProgress(progress / 100, data.message || data.state);

                // Update status
                Ext.getCmp('blindscan-status').setValue(data.message || data.state);

                // Update peak count
                Ext.getCmp('blindscan-peaks').setValue(data.peak_count || 0);
                Ext.getCmp('blindscan-muxes').setValue(data.muxes_created || 0);

                // Fetch spectrum data
                self.fetchSpectrum();

                // Continue polling if not complete
                if (data.state === 'acquiring' || data.state === 'scanning') {
                    setTimeout(function() { self.pollStatus(); }, 1000);
                } else {
                    // Scan complete
                    Ext.getCmp('blindscan-cancel-btn').disable();
                    Ext.getCmp('blindscan-create-btn').enable();
                    self.lastSession = self.session;  // Save for spectrum/peaks fetch
                    self.session = null;
                    self.starting = false;

                    // Fetch final spectrum and peaks
                    self.fetchSpectrum();
                    self.fetchPeaks();
                }
            }
        });
    },

    /**
     * Fetch spectrum data from all available bands
     */
    fetchSpectrum: function() {
        var self = this;
        var uuid = this.session || this.lastSession;

        if (!uuid) return;

        // Determine which polarisations to fetch based on what was scanned
        var pols = [];
        if (this.scanPol === 'B' || this.scanPol === 'H') pols.push('H');
        if (this.scanPol === 'B' || this.scanPol === 'V') pols.push('V');

        // Collect all spectrum data
        var allPoints = [];
        var pending = pols.length * 2;  // 2 bands per pol

        function checkComplete() {
            pending--;
            if (pending <= 0) {
                // Sort by frequency and update display
                allPoints.sort(function(a, b) { return a.f - b.f; });
                self.spectrumData = allPoints;
                self.drawSpectrum();
            }
        }

        // Fetch each polarisation and band combination
        pols.forEach(function(pol) {
            [0, 1].forEach(function(band) {  // 0=low, 1=high
                tvheadend.Ajax({
                    url: 'api/blindscan',
                    params: {
                        op: 'spectrum',
                        uuid: uuid,
                        polarisation: pol,
                        band: band
                    },
                    success: function(d) {
                        var data = Ext.decode(d.responseText);
                        if (data.points && data.points.length > 0) {
                            // Add pol/band info to each point for display
                            data.points.forEach(function(pt) {
                                pt.pol = pol;
                                pt.band = band;
                            });
                            allPoints = allPoints.concat(data.points);
                        }
                        checkComplete();
                    },
                    failure: function() {
                        checkComplete();
                    }
                });
            });
        });
    },

    /**
     * Fetch detected peaks and populate grid
     */
    fetchPeaks: function() {
        var self = this;
        var uuid = this.session || self.lastSession;

        if (!uuid) return;

        tvheadend.Ajax({
            url: 'api/blindscan',
            params: {
                op: 'peaks',
                uuid: uuid
            },
            success: function(d) {
                var data = Ext.decode(d.responseText);
                if (data.peaks) {
                    self.peaks = data.peaks;
                    self.drawSpectrum();

                    // Populate grid store
                    if (self.peaksStore) {
                        self.peaksStore.removeAll();
                        var records = [];
                        var newPeakIndices = [];
                        for (var i = 0; i < data.peaks.length; i++) {
                            var peak = data.peaks[i];
                            var isExisting = peak.existing || peak.status === 'existing';
                            records.push(new self.peaksStore.recordType({
                                selected: !isExisting,
                                frequency: peak.frequency,
                                symbol_rate: peak.symbol_rate,
                                level: peak.level,
                                snr: peak.snr || 0,
                                polarisation: peak.polarisation,
                                status: peak.status || 'pending',
                                modulation: peak.modulation || '',
                                fec: peak.fec || '',
                                rolloff: peak.rolloff || '',
                                pilot: peak.pilot || '',
                                pls_mode: peak.pls_mode || '',
                                pls_code: peak.pls_code || 0,
                                stream_id: peak.stream_id || -1,
                                delsys: peak.delsys || '',
                                existing: isExisting,
                                verified_freq: peak.verified_freq || 0,
                                verified_sr: peak.verified_sr || 0
                            }));
                            if (!isExisting) {
                                newPeakIndices.push(i);
                            }
                        }
                        self.peaksStore.add(records);

                        // Select only new (non-existing) peaks
                        var grid = Ext.getCmp('blindscan-peaks-grid');
                        if (grid && grid.getSelectionModel()) {
                            var sm = grid.getSelectionModel();
                            sm.clearSelections();
                            for (var j = 0; j < newPeakIndices.length; j++) {
                                sm.selectRow(newPeakIndices[j], true);
                            }
                        }
                    }
                }
            }
        });
    },

    /**
     * Cancel scan
     */
    cancelScan: function() {
        if (!this.session) return;

        tvheadend.Ajax({
            url: 'api/blindscan',
            params: {
                op: 'cancel',
                uuid: this.session
            }
        });

        this.lastSession = this.session;
        this.session = null;
        Ext.getCmp('blindscan-cancel-btn').disable();
        Ext.getCmp('blindscan-status').setValue(_('Cancelled'));
    },

    /**
     * Create muxes from selected peaks
     */
    createMuxes: function() {
        var self = this;
        var uuid = this.lastSession;

        if (!uuid) {
            Ext.Msg.alert(_('Error'), _('No scan session available'));
            return;
        }

        // Get selected peaks from grid
        var grid = Ext.getCmp('blindscan-peaks-grid');
        var selectedPeaks = [];
        if (grid && grid.getSelectionModel()) {
            var selections = grid.getSelectionModel().getSelections();
            for (var i = 0; i < selections.length; i++) {
                var sel = selections[i];
                selectedPeaks.push({
                    frequency: sel.get('frequency'),
                    polarisation: sel.get('polarisation'),
                    symbol_rate: sel.get('symbol_rate'),
                    modulation: sel.get('modulation'),
                    fec: sel.get('fec'),
                    rolloff: sel.get('rolloff'),
                    pilot: sel.get('pilot'),
                    stream_id: sel.get('stream_id'),
                    pls_mode: sel.get('pls_mode'),
                    pls_code: sel.get('pls_code'),
                    delsys: sel.get('delsys'),
                    is_gse: sel.get('is_gse')
                });
            }
        }

        if (selectedPeaks.length === 0) {
            Ext.Msg.alert(_('Error'), _('No peaks selected'));
            return;
        }

        // Check that all selected peaks have been prescanned
        var notScanned = [];
        for (var i = 0; i < selections.length; i++) {
            var status = selections[i].get('status');
            if (status !== 'locked' && status !== 'existing') {
                notScanned.push((selections[i].get('frequency') / 1000).toFixed(2) + ' MHz');
            }
        }
        if (notScanned.length > 0) {
            Ext.Msg.alert(_('Error'), _('Please prescan all selected peaks first') + ':\n' + notScanned.join(', '));
            return;
        }

        Ext.getCmp('blindscan-create-btn').disable();
        Ext.getCmp('blindscan-status').setValue(_('Creating muxes...'));

        tvheadend.Ajax({
            url: 'api/blindscan',
            params: {
                op: 'create_muxes',
                uuid: uuid,
                peaks: Ext.encode(selectedPeaks)
            },
            success: function(d) {
                var data = Ext.decode(d.responseText);
                var created = data.created || 0;
                Ext.getCmp('blindscan-muxes').setValue(created);
                Ext.getCmp('blindscan-status').setValue(_('Created') + ' ' + created + ' ' + _('muxes'));

                // Mark selected peaks as "existing" (they're now in TVH's mux list)
                var grid = Ext.getCmp('blindscan-peaks-grid');
                if (grid && grid.getSelectionModel()) {
                    var selections = grid.getSelectionModel().getSelections();
                    for (var i = 0; i < selections.length; i++) {
                        selections[i].set('status', 'existing');
                        selections[i].set('existing', true);
                        selections[i].commit();
                    }
                    // Clear selection and re-enable button
                    grid.getSelectionModel().clearSelections();
                }

                // Re-apply filter if "Hide existing" is checked
                var hideCheck = Ext.getCmp('blindscan-hide-existing');
                if (hideCheck && hideCheck.getValue() && self.peaksStore) {
                    self.peaksStore.filterBy(function(rec) {
                        return !rec.get('existing') && rec.get('status') !== 'existing';
                    });
                }

                Ext.getCmp('blindscan-create-btn').enable();
            },
            failure: function() {
                Ext.getCmp('blindscan-create-btn').enable();
                Ext.getCmp('blindscan-status').setValue(_('Failed to create muxes'));
            }
        });
    },

    /**
     * Prescan a peak to detect tuning parameters using Neumo blind tune
     */
    prescanPeak: function(rowIndex) {
        var self = this;
        var uuid = this.lastSession;

        if (!uuid) {
            Ext.Msg.alert(_('Error'), _('No scan session available'));
            return;
        }

        if (!this.peaksStore) {
            Ext.Msg.alert(_('Error'), _('No peaks data'));
            return;
        }

        var record = this.peaksStore.getAt(rowIndex);
        if (!record) {
            Ext.Msg.alert(_('Error'), _('Peak not found'));
            return;
        }

        var freq = record.get('frequency');
        var pol = record.get('polarisation');

        // Update status
        record.set('status', 'scanning');

        Ext.getCmp('blindscan-status').setValue(_('Prescanning') + ' ' + (freq / 1000).toFixed(2) + ' MHz...');

        tvheadend.Ajax({
            url: 'api/blindscan',
            params: {
                op: 'prescan',
                uuid: uuid,
                frequency: freq,
                polarisation: pol
            },
            success: function(d) {
                var data = Ext.decode(d.responseText);
                if (data.locked) {
                    record.set('status', 'locked');
                    record.set('symbol_rate', data.symbol_rate || 0);
                    record.set('modulation', data.modulation || '');
                    record.set('fec', data.fec || '');
                    record.set('rolloff', data.rolloff || '');
                    record.set('pilot', data.pilot || '');
                    record.set('pls_mode', data.pls_mode || '');
                    record.set('pls_code', data.pls_code || 0);
                    record.set('stream_id', data.stream_id !== undefined ? data.stream_id : -1);
                    record.set('delsys', data.delsys || '');
                    record.set('is_gse', data.is_gse || false);
                    if (data.frequency) {
                        record.set('frequency', data.frequency);
                    }
                    record.commit();

                    // Handle multistream - add additional rows for each ISI
                    if (data.multistream && data.isi_list && data.isi_list.length > 1) {
                        // The first ISI is already in the current record
                        // Add remaining ISIs as new entries
                        for (var i = 1; i < data.isi_list.length; i++) {
                            var isi = data.isi_list[i];
                            var newRecord = new self.peaksStore.recordType({
                                selected: true,
                                frequency: data.frequency,
                                symbol_rate: data.symbol_rate,
                                level: record.get('level'),
                                snr: record.get('snr'),
                                polarisation: pol,
                                status: 'locked',
                                modulation: data.modulation || '',
                                fec: data.fec || '',
                                rolloff: data.rolloff || '',
                                pilot: data.pilot || '',
                                pls_mode: data.pls_mode || '',
                                pls_code: data.pls_code || 0,
                                stream_id: isi,
                                delsys: data.delsys || '',
                                multistream: true,
                                is_gse: data.is_gse || false
                            });
                            self.peaksStore.add(newRecord);
                        }

                        Ext.getCmp('blindscan-status').setValue(
                            _('Multistream') + ': ' + (data.frequency / 1000).toFixed(2) + ' MHz, ' +
                            data.isi_list.length + ' ISIs'
                        );
                    } else {
                        Ext.getCmp('blindscan-status').setValue(
                            _('Locked') + ': ' + (data.frequency / 1000).toFixed(2) + ' MHz, ' +
                            (data.symbol_rate / 1000).toFixed(0) + ' ksps, ' +
                            data.delsys + ' ' + data.modulation + ' ' + data.fec
                        );
                    }
                } else {
                    record.set('status', 'failed');
                    record.commit();
                    Ext.getCmp('blindscan-status').setValue(
                        _('Failed to lock') + ' ' + (freq / 1000).toFixed(2) + ' MHz'
                    );
                }
            },
            failure: function() {
                record.set('status', 'failed');
                record.commit();
                Ext.getCmp('blindscan-status').setValue(_('Prescan failed'));
            }
        });
    },

    /**
     * Draw spectrum on canvas
     */
    drawSpectrum: function() {
        if (!this.canvas) return;

        var ctx = this.canvas.getContext('2d');
        var width = this.canvas.width;
        var height = this.canvas.height;

        // Clear canvas
        ctx.fillStyle = '#1a1a1a';
        ctx.fillRect(0, 0, width, height);

        // Draw grid
        ctx.strokeStyle = '#333';
        ctx.lineWidth = 1;

        // Horizontal grid lines
        for (var i = 0; i <= 5; i++) {
            var y = height * i / 5;
            ctx.beginPath();
            ctx.moveTo(0, y);
            ctx.lineTo(width, y);
            ctx.stroke();
        }

        // Vertical grid lines (frequency markers)
        for (var i = 0; i <= 10; i++) {
            var x = width * i / 10;
            ctx.beginPath();
            ctx.moveTo(x, 0);
            ctx.lineTo(x, height);
            ctx.stroke();
        }

        // Draw spectrum
        if (this.spectrumData.length > 0) {
            // Find min/max levels
            var minLevel = Infinity, maxLevel = -Infinity;
            var minFreq = Infinity, maxFreq = -Infinity;

            for (var i = 0; i < this.spectrumData.length; i++) {
                var pt = this.spectrumData[i];
                if (pt.l < minLevel) minLevel = pt.l;
                if (pt.l > maxLevel) maxLevel = pt.l;
                if (pt.f < minFreq) minFreq = pt.f;
                if (pt.f > maxFreq) maxFreq = pt.f;
            }

            // Ensure we have a valid range (avoid division by zero)
            var levelRange = maxLevel - minLevel;
            if (levelRange < 1) levelRange = 1;  // Avoid div by zero
            var freqRange = maxFreq - minFreq;
            if (freqRange < 1) freqRange = 1;

            // Debug info on canvas
            ctx.fillStyle = '#888';
            ctx.font = '9px sans-serif';
            ctx.fillText('Points: ' + this.spectrumData.length + ', Level range: ' + minLevel + ' to ' + maxLevel, 5, 12);

            // Draw spectrum line
            ctx.strokeStyle = '#00ff00';
            ctx.lineWidth = 1;
            ctx.beginPath();

            for (var i = 0; i < this.spectrumData.length; i++) {
                var pt = this.spectrumData[i];
                var x = (pt.f - minFreq) / freqRange * width;
                // Invert Y: high level at top, low at bottom
                var y = height - ((pt.l - minLevel) / levelRange * (height - 20)) - 10;

                if (i === 0) {
                    ctx.moveTo(x, y);
                } else {
                    ctx.lineTo(x, y);
                }
            }
            ctx.stroke();

            // Draw frequency labels
            ctx.fillStyle = '#fff';
            ctx.font = '10px sans-serif';
            ctx.fillText((minFreq / 1000).toFixed(1) + ' MHz', 5, height - 5);
            ctx.fillText((maxFreq / 1000).toFixed(1) + ' MHz', width - 70, height - 5);
        } else {
            // No data message
            ctx.fillStyle = '#666';
            ctx.font = '14px sans-serif';
            ctx.fillText('No spectrum data available', width/2 - 80, height/2);
        }

        // Draw peaks as triangles
        if (this.peaks && this.peaks.length > 0 && this.spectrumData.length > 0) {
            var minFreq = this.spectrumData[0].f;
            var maxFreq = this.spectrumData[this.spectrumData.length - 1].f;

            ctx.fillStyle = '#ff0000';
            for (var i = 0; i < this.peaks.length; i++) {
                var peak = this.peaks[i];
                var x = (peak.frequency - minFreq) / (maxFreq - minFreq) * width;

                // Draw small triangle
                ctx.beginPath();
                ctx.moveTo(x, 10);
                ctx.lineTo(x - 5, 0);
                ctx.lineTo(x + 5, 0);
                ctx.fill();
            }
        }
    }
};
