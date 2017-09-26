var LAST_STEP = 9;
var currentStep = undefined;

function runQuery(categoryName, buttonId) {
    return function() {
        var data = {query_name: categoryName};
        $.post('/run_query', JSON.stringify(data), function () {
            var btn = $(buttonId);
            btn.text('Query submitted!');
            btn.removeClass('btn-primary btn-danger').addClass('btn-success');
        });
    }
}

function navigateToTop() {
    document.body.scrollTop = 0; // For Chrome, Safari and Opera
    document.documentElement.scrollTop = 0; // For IE and Firefox
}

function activateStep(step) {
    $('.navigationStep').parent().removeClass('active');
    $('[data-tab-index=' + step + ']').parent().addClass('active');
    $('[id^=tab]').removeClass('active');
    $('#tab' + step).addClass('active');
}

function collapsePanel(panel) {
    var $this = $(this);
    if(!$this.hasClass('panel-collapsed')) {
        $this.parents('.panel').find('.panel-body').slideUp();
        $this.addClass('panel-collapsed');
        $this.find('i').removeClass('glyphicon-chevron-up').addClass('glyphicon-chevron-down');
    } else {
        $this.parents('.panel').find('.panel-body').slideDown();
        $this.removeClass('panel-collapsed');
        $this.find('i').removeClass('glyphicon-chevron-down').addClass('glyphicon-chevron-up');
    }
}

function refreshNextPrevButtons() {
    if (currentStep === 1) {
        $('#prevButton').hide();
    } else {
        $('#prevButton').show();
    }
    if (currentStep === LAST_STEP) {
        $('#nextButton').hide();
    } else {
        $('#nextButton').show();
    }
}

function moveToStep(step) {
    if (step <= LAST_STEP && step > 0) {
        $.post('/step', JSON.stringify({step: step}), function (data) {
            activateStep(data['current_step']);
            currentStep = data['current_step'];
            navigateToTop();
            refreshNextPrevButtons();
        });
    }
}

function registerEvents() {
    $(document).on('click', '.panel-heading span.clickable-panel', collapsePanel);
    $('#topCategoriesButton').on('click', runQuery('top_categories', '#topCategoriesButton'));
    $('#topProductsButton').on('click', runQuery('top_products', '#topProductsButton'));
    $('#topRevenueButton').on('click', runQuery('top_revenue', '#topRevenueButton'));
    $('#webLogsButton').on('click', runQuery('web_logs', '#webLogsButton'));
    $('#importTablesButton').on('click', function() {
        $.post('/import_tables', function () {
            var btn = $('#importTablesButton');
            btn.text('Import initiated!');
            btn.removeClass('btn-primary').addClass('btn-success');
        }).fail(function() {
            $('#dataStoreAlert').show();
        });
    });
    $('#scalingButton').on('click', function() {
        $.post('/run_scaling', function () {
            var btn = $('#scalingButton');
            btn.text('Queries submitted!');
            btn.removeClass('btn-primary').addClass('btn-success');
        }).fail(function() {
        });
    });
    $('#createClusters').on('click', function() {
        var btn = $(this);
        btn.removeClass('btn-primary btn-danger').addClass('btn-primary');
        btn.button('loading');
        $.post('/create_clusters_and_notebooks', function (response) {
            if(response != null && response != undefined &&
                response.error != null) {
                $('#createClustersAndNotebooksErrorMessage').html(response.error.error_message)
                $('#createClustersAndNotebooksAlert').show();
                btn.button('reset');
                btn.removeClass('btn-primary btn-danger').addClass('btn-danger');
            }
            else {
                btn.text('Clusters were created and are starting!');
                btn.removeClass('btn-primary btn-danger').addClass('btn-success');
            }
        }).fail(function(response) {
            console.log(response)
            btn.button('reset');
            btn.removeClass('btn-primary btn-danger').addClass('btn-danger');
        });
    });
    $('#closeAlertButton').on('click', function() { $('#dataStoreAlert').hide(); });
    $('.navigationStep').on('click', function(){
        var step = parseInt($(this).attr('data-tab-index'));
        moveToStep(step);
    });
    $('#nextButton').on('click', function() { moveToStep(currentStep + 1); });
    $('#prevButton').on('click', function() { moveToStep(currentStep - 1); });
}

$(document).ready(function() {
    $.get('/step', function(data) {
        currentStep = parseInt(data['current_step']);
        console.log(currentStep);
        registerEvents();
        activateStep(currentStep);
        refreshNextPrevButtons();
    });
});