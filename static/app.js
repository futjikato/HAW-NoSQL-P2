(function(window, $, UIkit) {
    var postalMap,
        postalMapMarker,
        postalInfoWindow;

    window.initMap = function initMap() {
        postalMap = new google.maps.Map($('.js-map-postal').get(0), {
            center: {lat: -34.397, lng: 150.644},
            zoom: 8
        });
        postalMapMarker = new google.maps.Marker();
        postalInfoWindow = new google.maps.InfoWindow();
        postalMapMarker.addListener('click', function() {
            postalInfoWindow.open(postalMap, postalMapMarker);
        });
    };

    function handlePostalData(data, inputElem, errorElem) {
        if (data.err) {
            inputElem.addClass('uk-form-danger');
            errorElem.text(data.err).removeClass('uk-hidden');

            postalMapMarker.setMap(null);
            return;
        }

        inputElem.removeClass('uk-form-danger');
        errorElem.addClass('uk-hidden');

        var posAry;
        if (data.lat && data.lng) {
            posAry = [
                parseFloat(data.lat),
                parseFloat(data.lng)
            ];
        } else {
            if (Array.isArray(data.loc)) {
                posAry = data.loc;
            } else {
                posAry = JSON.parse(data.loc);
            }
        }

        postalMapMarker.setPosition({lat: posAry[1], lng: posAry[0]});
        postalMapMarker.setTitle(data.city);
        postalMapMarker.setMap(postalMap);

        postalInfoWindow.setContent('<h2>'+data.city+'</h2><dl><dt>State</dt><dd>'+data.state+'</dd><dt>Population</dt><dd>'+data.pop+'</dd></dl>');

        postalMap.setCenter({lat: posAry[1], lng: posAry[0]});
        postalMap.setZoom(10);
    }

    var backend = 'hbase';
    $('.js-backend').on('click', function(e) {
        e.preventDefault();

        $('.js-backend.uk-active').removeClass('uk-active');
        $(this).addClass('uk-active');

        backend = $(this).data('backend');
    });

    $('.js-search-postal').on('submit', function(e) {
        e.preventDefault();

        var postalElem = $('input[name="postal"]');
        var postalErrorElem = $('.js-error-postal');

        $.getJSON('/postal/' + backend + '/' + postalElem.val(), function(data) {
            handlePostalData(data, postalElem, postalErrorElem);
        });
    });

    $('.js-search-city').on('submit', function(e) {
        e.preventDefault();

        var cityElem = $('input[name="city"]');
        var selectionElem = $('.js-selection');

        $.getJSON('/city/' + backend + '/' + cityElem.val(), function(data) {
            data.forEach(function(keyData, i) {
                var label;
                if (keyData.plz) {
                    label = keyData.plz;
                } else if (keyData._id) {
                    label = keyData._id;
                } else {
                    label = '#'+i;
                }
                selectionElem.append($('<option>').attr('value', JSON.stringify(keyData)).text(label));
            });
        });
    });
    
    $('.js-selection').on('change', function(e) {
        var elem = $(this);
        var cityErrorElem = $('.js-error-city');
        var cityElem = $('input[name="city"]');
        var data = JSON.parse(elem.val());
        handlePostalData(data, cityElem, cityErrorElem);
    });
})(window, jQuery, UIkit);
