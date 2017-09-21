import 'dart:async';
import 'dart:math';
import 'package:offthechain/offthechain.dart';
import 'package:logging/logging.dart' as log;
import 'datastore_api.dart' as datastore;
import 'google_cloud_datastore_service.dart';

/// A Gram Repo which relies on Google Cloud Datastore
class GoogleCloudDatastoreGramRepository<T extends GramItem>
    implements GramRepository<T> {

  static final log.Logger _logger = new log.Logger('GoogleCloudDatastoreGramRepository');
  GoogleCloudDatastoreService _cloudService;
  final Random _random = new Random();

  /// Instantiates a new Gram repository using Google Cloud Datastore
  GoogleCloudDatastoreGramRepository(String privateKeyId, String privateKey,
      String clientEmail, String clientId, String projectId, String namespaceId) {

    _cloudService = new GoogleCloudDatastoreService(privateKeyId, privateKey,
        clientEmail, clientId, projectId, namespaceId, "Gram");
  }

  @override
  Future<Gram<T>> getByTextPrefix(List<T> items, List<String> corpusIds) async {

    await _cloudService.apiReady;

    final List<Gram<T>> grams = new List<Gram<T>>();
    final String comparableItemsString = "${_getComparableItemsString(items)}-";
    _logger.fine("Getting Gram with comparable string prefix $comparableItemsString");

    for (String corpusId in corpusIds) {

      final datastore.Filter comparableItemsStringFilter1 = _cloudService
          .getFilter(
          "comparableItemsString", "GREATER_THAN",
          new datastore.Value()..stringValue = comparableItemsString);

      final datastore.Filter comparableItemsStringFilter2 = _cloudService
          .getFilter(
          "comparableItemsString", "LESS_THAN",
          new datastore.Value()..stringValue = "$comparableItemsString\uFFFD");

      final datastore.Filter corpusIdFilter = _cloudService.getFilter(
          "corpusId",
          "EQUAL", new datastore.Value()..stringValue = corpusId);

      final datastore.Filter isStartOfChainFilter = _cloudService.getFilter(
          "isStartOfChain",
          "EQUAL", new datastore.Value()..booleanValue = false);

      final datastore.CompositeFilter compositeFilter = new datastore
          .CompositeFilter()
        ..op = "AND"
        ..filters = new List<datastore.Filter>()
        ..filters.add(comparableItemsStringFilter1)
        ..filters.add(comparableItemsStringFilter2)
        ..filters.add(isStartOfChainFilter)
        ..filters.add(corpusIdFilter);

      final datastore.Filter filter = new datastore.Filter()
        ..compositeFilter = compositeFilter;

      final datastore.Query query = _cloudService.getQuery("Gram", filter);

      List<datastore.Entity> entities;
      await _cloudService.tryWithBackoff(() async => entities =
          await _cloudService.getEntities(query));
      entities.forEach((datastore.Entity entity) {
        final Gram<T> gram = _entity2gram(entity);
        _logger.fine("Found possible gram with ID ${gram.id} and Corpus ID ${gram.corpusId}");
        grams.add(gram);
      });
    }
    try {
      return Gram.selectGramByProbability<T>(grams);
    } catch (e) {
      _logger.severe("Failed to find a Gram to follow $comparableItemsString");
      rethrow;
    }
  }

  @override
  Future<Gram<T>> getByItems(List<T> items, String corpusId) async {

    await _cloudService.apiReady;

    final String comparableItemsString = _getComparableItemsString(items);
    _logger.fine("Getting Gram by items $comparableItemsString");

    final datastore.Filter comparableItemsStringFilter = _cloudService.getFilter(
        "comparableItemsString", "EQUAL",
        new datastore.Value()..stringValue = comparableItemsString);

    final datastore.Filter corpusIdFilter = _cloudService.getFilter("corpusId", 
        "EQUAL", new datastore.Value()..stringValue = corpusId);

    final datastore.CompositeFilter compositeFilter = new datastore.CompositeFilter()
      ..op = "AND"
      ..filters = new List<datastore.Filter>()
      ..filters.add(comparableItemsStringFilter)
      ..filters.add(corpusIdFilter);

    final datastore.Filter filter = new datastore.Filter()
      ..compositeFilter = compositeFilter;

    final datastore.Query query = _cloudService.getQuery("Gram", filter);

    datastore.Entity entity;
    await _cloudService.tryWithBackoff(() async => entity =
        await _cloudService.getSingleEntity(query));
    
    if (entity == null) {
      return null;
    }

    final Gram<T> gram = _entity2gram(entity);

    return gram;
  }

  @override
  Future<dynamic> deleteById(String id) {
    _logger.fine("Deleting gram with ID $id");
    throw new Exception("Not implemented");
  }

  @override
  Future<dynamic> save(List<Gram<T>> grams, {bool updateTimestamp = true}) async {

    // Sigh, make sure the list of Grams to save is unique in case some chucklehead
    // done passed in a list with duplicate entries
    final Map<String, Gram<T>> gramsToSave = new Map<String, Gram<T>>();
    for(Gram<T> gram in grams) {
      if (gramsToSave[gram.id] == null) {
        gramsToSave[gram.id] = gram;
      }
    }

    DateTime updateTime;

    await _cloudService.apiReady;

    final List<datastore.Mutation> mutations = new List<datastore.Mutation>();

    for(Gram<T> gram in gramsToSave.values) {

      _logger.fine("Saving Gram with id ${gram.id}");

      // Normally we want to update the "updated" timestamp on a Gram when saving
      // it, but if the caller has requested not to, and it's not a new object,
      // then we won't.
      if (!updateTimestamp && !gram.isNew) {
        updateTime = gram.updated;
      } else {
        updateTime = new DateTime.now().toUtc();
      }

      final datastore.ArrayValue innerItemStringsArrayValue = new datastore
          .ArrayValue()
        ..values = new List<datastore.Value>();
      for (T item in gram.items) {
        innerItemStringsArrayValue.values.add(new datastore.Value()
          ..excludeFromIndexes = true
          ..stringValue = item.toString());
      }
      final String comparableItemsString = _getComparableItemsString(
          gram.items);

      final datastore.Entity entity = new datastore.Entity()
        ..key = _cloudService.getKey(gram.id)
        ..properties = <String, datastore.Value>{
          "created": new datastore.Value()
            ..excludeFromIndexes = false
            ..timestampValue = gram.created.toIso8601String(),
          "updated": new datastore.Value()
            ..excludeFromIndexes = false
            ..timestampValue = updateTime.toIso8601String(),
          "corpusId": new datastore.Value()
            ..excludeFromIndexes = false
            ..stringValue = gram.corpusId,
          "occurrences": new datastore.Value()
            ..excludeFromIndexes = true
            ..integerValue = gram.occurrences.toString(),
          "innerItemStrings": new datastore.Value()
            ..arrayValue = innerItemStringsArrayValue,
          "comparableItemsString": new datastore.Value()
            ..stringValue = comparableItemsString,
          "isStartOfChain": new datastore.Value()
            ..excludeFromIndexes = false
            ..booleanValue = gram.isStartOfChain,
          "isEndOfChain": new datastore.Value()
            ..excludeFromIndexes = true
            ..booleanValue = gram.isEndOfChain,
          "randomNumber": new datastore.Value()
            ..excludeFromIndexes = false
            ..integerValue = _random.nextInt(2147483647).toString()
        };

      final datastore.Mutation mutation = new datastore.Mutation();
      if (gram.isNew) {
        mutation.insert =
            entity;
      } else {
        mutation.update = entity;
      }

      mutations.add(mutation);
    }

    final datastore.CommitRequest cr = new datastore.CommitRequest()
      ..mode = "NON_TRANSACTIONAL"
      ..mutations = mutations;

    await _cloudService.tryWithBackoff(() async {
      _logger.fine("About to save gram(s)");
      await _cloudService.api.projects.commit(cr, _cloudService.projectId);
      _logger.fine("Saved gram(s)");
    });

    for(Gram<T> gram in grams) {
      gram.updated = updateTime;
      _logger.fine("Saved Gram ${gram.id}");
    }
  }

  @override
  Future<Gram<T>> getRandomStartingGram(List<String> corpusIds) async {

    _logger.fine("Getting a random starting gram for Corpus IDs ${corpusIds.join(", ")}");

    await _cloudService.apiReady;

    final List<Gram<T>> possibleGrams = new List<Gram<T>>();
    for (String corpusId in corpusIds) {


      final datastore.Filter corpusIdFilter = _cloudService.getFilter(
          "corpusId",
          "EQUAL", new datastore.Value()..stringValue = corpusId);

      final datastore.Filter isStartOfChainFilter = _cloudService.getFilter(
          "isStartOfChain",
          "EQUAL", new datastore.Value()..booleanValue = true);

      final datastore.CompositeFilter compositeFilter = new datastore
          .CompositeFilter()
        ..op = "AND"
        ..filters = new List<datastore.Filter>()
        ..filters.add(corpusIdFilter)
        ..filters.add(isStartOfChainFilter);

      final datastore.Filter filter = new datastore.Filter()
        ..compositeFilter = compositeFilter;

      final datastore.PropertyReference orderPropertyReference = new datastore
          .PropertyReference()
        ..name = "randomNumber";
      final datastore.PropertyOrder propertyOrder = new datastore
          .PropertyOrder()
        ..property = orderPropertyReference;
      final datastore.Query query = _cloudService.getQuery("Gram", filter)
        ..limit = 1
        ..order = <datastore.PropertyOrder>[propertyOrder];

      datastore.Entity entity;
      await _cloudService.tryWithBackoff(() async => entity =
          await _cloudService.getSingleEntity(query));

      if (entity == null) {
        return null;
      }

      final Gram<T> gram = _entity2gram(entity);

      // OK, we found one, so let's save it again so it gets a new random number.
      await save(<Gram<T>>[gram], updateTimestamp: false);

      possibleGrams.add(gram);
    }

    // Now return one of the random ones we found.
    return possibleGrams[_random.nextInt(possibleGrams.length)];
  }

  @override
  Future<List<Gram<T>>> getAll() {
    _logger.fine("Getting all Grams");
    throw new Exception("Not implemented");
  }

  @override
  Future<Gram<T>> getById(String id) {
    _logger.fine("Getting Gram with ID $id");
    throw new Exception("Not implemented");
  }

  String _getComparableItemsString(List<T> items) {
    final List<String> comparableItemsStringWords = new List<String>();
    for(T item in items) {
      comparableItemsStringWords.add(item.comparableString);
    }
    return comparableItemsStringWords.join("-");
  }

  Gram<T> _entity2gram(datastore.Entity entity) {
    final Gram<T> gram = new Gram<T>(
        entity.properties["corpusId"].stringValue,
        int.parse(entity.properties["occurrences"].integerValue),
        id: entity.key.path[0].name,
        created: DateTime.parse(entity.properties["created"].timestampValue),
        updated: DateTime.parse(entity.properties["updated"].timestampValue),
        isStartOfChain: entity.properties["isStartOfChain"].booleanValue,
        isEndOfChain: entity.properties["isEndOfChain"].booleanValue
    );

    for (datastore.Value values in entity.properties["innerItemStrings"].arrayValue.values) {
      // TODO: Figure out how to not hard code "GramString" here.
      final GramItem gramString = new GramString(values.stringValue);
      gram.addItem(gramString);
    }

    return gram;
  }
}