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

        var posAry = JSON.parse(data.loc);
        postalMapMarker.setPosition({lat: posAry[1], lng: posAry[0]});
        postalMapMarker.setTitle(data.city);
        postalMapMarker.setMap(postalMap);

        postalInfoWindow.setContent('<h2>'+data.city+'</h2><dl><dt>State</dt><dd>'+data.state+'</dd><dt>Population</dt><dd>'+data.pop+'</dd></dl>');

        postalMap.setCenter({lat: posAry[1], lng: posAry[0]});
        postalMap.setZoom(10);
    }

    $('.js-search-postal').on('submit', function(e) {
        e.preventDefault();

        var postalElem = $('input[name="postal"]');
        var postalErrorElem = $('.js-error-postal');

        $.getJSON('/postal/' + postalElem.val(), function(data) {
            handlePostalData(data, postalElem, postalErrorElem);
        });
    });

    $('.js-search-city').on('submit', function(e) {
        e.preventDefault();

        var cityElem = $('input[name="city"]');
        var cityErrorElem = $('.js-error-city');
        var selectionElem = $('.js-selection');

        $.getJSON('/city/' + cityElem.val(), function(data) {
            data.forEach(function(keyData) {
                selectionElem.append($('<option>').attr('value', JSON.stringify(keyData)).text(keyData.plz));
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

    var postalElem = $('input[name="postal"]')
    UIkit.autocomplete(postalElem.get(0), { source: function() {
        return '/search/postal/'+postalElem.val();
    }});
})(window, jQuery, UIkit);
