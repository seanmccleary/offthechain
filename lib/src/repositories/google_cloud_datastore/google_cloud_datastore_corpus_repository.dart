import 'dart:async';
import 'package:offthechain/offthechain.dart';
import 'package:logging/logging.dart' as log;
import 'datastore_api.dart' as datastore;
import 'google_cloud_datastore_service.dart';

/// A Corpus repository using Google Cloud Datastore as the data source
class GoogleCloudDatastoreCorpusRepository implements CorpusRepository {

  static final log.Logger _logger = new log.Logger('GoogleCloudDatastoreCorpusRepository');
  GoogleCloudDatastoreService _cloudService;

    /// Instantiates a new Corpus repository using Google Cloud Datastore
  GoogleCloudDatastoreCorpusRepository(String privateKeyId, String privateKey,
      String clientEmail, String clientId, String projectId, String namespaceId) {

    _cloudService = new GoogleCloudDatastoreService(privateKeyId, privateKey,
        clientEmail, clientId, projectId, namespaceId, "Corpus");
  }

  @override
  Future<Corpus> getById(String id) async {
    _logger.fine("Getting Corpus with ID $id");

    await _cloudService.apiReady;

    final datastore.ReadOptions readOptions = new datastore.ReadOptions()
      ..readConsistency = "STRONG";
    final datastore.LookupRequest request = new datastore.LookupRequest()
      ..keys = <datastore.Key>[_cloudService.getKey(id)]
      ..readOptions = readOptions;

    datastore.LookupResponse response;
    await _cloudService.tryWithBackoff(() async => response =
        await _cloudService.api.projects.lookup(request, _cloudService.projectId));

    if(response.found == null || response.found.isEmpty) {
      return null;
    }
    final datastore.Entity result = response.found.first.entity;

    return _entity2corpus(result);
  }

  @override
  Future<Corpus> getByName(String name) async {
    _logger.fine("Getting Corpus with name $name");

    final datastore.Filter filter = _cloudService.getFilter("name", "EQUAL",
        new datastore.Value()..stringValue = name);
    final datastore.Query query = _cloudService.getQuery("Corpus", filter);

    datastore.Entity entity;
    await _cloudService.tryWithBackoff(() async => entity =
        await _cloudService.getSingleEntity(query));

    if (entity == null) {
      return null;
    } else {
      return _entity2corpus(entity);
    }
  }

  @override
  Future<List<Corpus>> getAll() async {
    _logger.fine("Getting all Corpuses");
    throw new Exception("Not implemented");
  }

  @override
  Future<dynamic> deleteById(String id) async {
    _logger.fine("Deleting Corpus with ID $id");
    throw new Exception("Not implemented");
  }

  @override
  Future<dynamic> save(List<Corpus> corpuses) async {

    // TODO: Change this to save all the corpopses in one batch, like GramRepo does
    for(Corpus corpus in corpuses) {
      _logger.fine("Saving Corpus with id  ${corpus.id}");
      final DateTime updateTime = new DateTime.now().toUtc();

      await _cloudService.apiReady;

      final datastore.Entity entity = new datastore.Entity()
        ..key = _cloudService.getKey(corpus.id)
        ..properties = <String, datastore.Value> {
          "name": new datastore.Value()
            ..excludeFromIndexes = false
            ..stringValue = corpus.name,
          "created": new datastore.Value()
            ..excludeFromIndexes = false
            ..timestampValue = corpus.created.toIso8601String(),
          "updated": new datastore.Value()
            ..excludeFromIndexes = false
            ..timestampValue = updateTime.toIso8601String(),
        };

      final datastore.Mutation mutation = new datastore.Mutation();
      if (corpus.isNew) {
        mutation.insert =
            entity;
      } else {
        mutation.update = entity;
      }

      final datastore.CommitRequest cr = new datastore.CommitRequest()
        ..mode = "NON_TRANSACTIONAL"
        ..mutations = <datastore.Mutation>[mutation];

      // ignore: unused_local_variable
      datastore.CommitResponse response;
      await _cloudService.tryWithBackoff(() async => response =
          await _cloudService.api.projects.commit(cr, _cloudService.projectId));

      corpus.updated = updateTime;
      _logger.fine("Saved Corpus ${corpus.id}");
    }
  }

  Corpus _entity2corpus(datastore.Entity entity) => new Corpus(
      entity.properties["name"].stringValue,
      id: entity.key.path[0].name,
      created: DateTime.parse(entity.properties["created"].timestampValue),
      updated: DateTime.parse(entity.properties["updated"].timestampValue)
  );
}