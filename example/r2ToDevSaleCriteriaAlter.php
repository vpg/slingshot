<?php
require_once __DIR__ . '/../vendor/autoload.php';
use \vpg\slingshot;

/**
 * Elastic hosts configuration
 * Here we work on the same host
 */
$esCnfHash = [
    'from' => 'vp-ol-provider-rec2.recette.vpg.lan:9200',
    'to'   => 'vp-ol-provider-rec2.recette.vpg.lan:9200'
];

$version = [
    'code'  => 1,
    'label' => 'Switch criteria_filters pension code and label values, Replace criteri_filters "_c.de" key to "_c_de" (regions)'
];

/**
 * Return true if the doc has already been migrated
 *
 * @param $docHash Document Hash taken from ELS
 * @param $version Version to check for the document
 *
 * @return bool True if the doc has already been migrated to the required version
 */
function alreadyMigrated($docHash, $version) {

    if (isset($docHash['tech'])) {
        $techHash = $docHash['tech'];
        foreach($techHash as $techValueHash) {
            if ($techValueHash['code'] == $version['code']) {
                return true;
            }
        }
    }

    return false;
};

/**
 * Replaces all docs of the given index
 * Each new doc is returned by enrichDocCallBack which :
 *     -  Adds 'gar' and 'DT' fields
 *     -  Removes 'foo' field 
 */
$enrichDocCallBack = function ($docHash) {

    global $version;

    // If the
    if (alreadyMigrated($docHash, $version)) {
        return $docHash;
    }

    $saleNb = count($docHash['bus']);
    for( $saleI = 0; $saleI < $saleNb; $saleI++ ) {

        $localesListHash = $docHash['bus'][$saleI]['locales'];
        foreach($localesListHash as $keyLocales => $localesHash) {
            $criteriaFiltersListHash = $localesHash['criteria_filters'];

            foreach($criteriaFiltersListHash as $keyCriteriaFilters => $criteriaFiltersHash)
            {
                // Pensions
                if ($criteriaFiltersHash['name'] == 'c.pe')
                {
                    $label = $criteriaFiltersHash['code'];
                    $code = $criteriaFiltersHash['label'];
                    $docHash['bus'][$saleI]['locales'][$keyLocales]['criteria_filters'][$keyCriteriaFilters]['label'] = $label;
                    $docHash['bus'][$saleI]['locales'][$keyLocales]['criteria_filters'][$keyCriteriaFilters]['code'] = $code;

                }
                elseif ($criteriaFiltersHash['name'] == '_c.de')
                { // Destinations
                    $docHash['bus'][$saleI]['locales'][$keyLocales]['criteria_filters'][$keyCriteriaFilters]['name'] = '_c_de';
                }
            }
        }
    }

    // Update tech key in order to have history
    $docHash['tech'][] = $version;

    return $docHash;
};

/**
 * Migration params
 * Here we replace all doc on the given index
 */
$migrationConfHash = [
    'from' => ['index' => 'sale', 'type' => 'details', 'size' => 50],
    'to'   => ['index' => 'sale', 'type' => 'details'],
    'bulk' => ['action' => 'index', 'batchSize' => 500],
    'documentsBatch' => ['batchNb' => 1, 'batchSize' => 1],
    'withScroll' => true
];

// Query to filter documents to process
$queryHash = [];

// Run the migration
$slingshot = new slingshot\Slingshot($esCnfHash, $migrationConfHash);
$slingshot->migrate($queryHash, $enrichDocCallBack);
