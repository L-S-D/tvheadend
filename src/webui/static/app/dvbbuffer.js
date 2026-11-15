tvheadend.dvbbuffer = function(panel, index) {

    tvheadend.idnode_simple(panel, {
        url: 'api/dvbbuffer/config',
        title: _('Instant zapping'),
        iconCls: 'film_edit',
        tabIndex: index,
        comet: 'dvbbuffer',
        labelWidth: 220,
        width: 570
    });

};
