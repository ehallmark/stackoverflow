


$(document).ready(function() {
    $('form.error_form').submit(function(e) {
        e.preventDefault();
        var data = $(this).serialize();
        $.ajax({
            url: '/recommend',
            data: data,
            dataType: 'json',
            type: 'POST',
            success: function(result) {
                $('#results').html(result.data);
                $('#results .answer-body[data-html]').each(function() {
                    $(this).html($(this).attr('data-html'));
                });
            }
        });
    });

    $('select.beer_selection').select2({
        minimumResultsForSearch: 10,
        closeOnSelect: true,
        placeholder: 'Select multiple beers...',
        ajax: {
            url: '/ajax/beers',
            dataType: 'json'
            // Additional AJAX parameters go here; see the end of this chapter for the full code of this example
        }
    });
    $('select.brewery_selection').select2({
        minimumResultsForSearch: 10,
        closeOnSelect: true,
        placeholder: 'Select multiple breweries...',
        ajax: {
            url: '/ajax/breweries',
            dataType: 'json'
            // Additional AJAX parameters go here; see the end of this chapter for the full code of this example
        }
    });
});