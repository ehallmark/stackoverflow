


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

    $('select.select_tags').select2({
        minimumResultsForSearch: 10,
        closeOnSelect: true,
        placeholder: 'Select relevant tags...',
        ajax: {
            url: '/ajax/tags',
            dataType: 'json'
            // Additional AJAX parameters go here; see the end of this chapter for the full code of this example
        }
    });

    $('select.error_search').select2({
        minimumResultsForSearch: 10,
        closeOnSelect: true,
        placeholder: 'Search Error Codes...',
        ajax: {
            url: '/ajax/errors',
            dataType: 'json'
            // Additional AJAX parameters go here; see the end of this chapter for the full code of this example
        }
    });

    $('select.exception_search').select2({
        minimumResultsForSearch: 10,
        closeOnSelect: true,
        placeholder: 'Search Exceptions...',
        ajax: {
            url: '/ajax/exceptions',
            dataType: 'json'
            // Additional AJAX parameters go here; see the end of this chapter for the full code of this example
        }
    });
});