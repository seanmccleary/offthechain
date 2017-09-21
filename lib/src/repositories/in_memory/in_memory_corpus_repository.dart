import 'dart:async';
import 'package:dddart/dddart.dart';
import 'package:logging/logging.dart' as log;
import '../../corpus.dart';
import '../corpus_repository.dart';

/// An in-memory implementation of the Corpus repository
class InMemoryCorpusRepository extends InMemoryAggregateRootRepository<Corpus>
    implements CorpusRepository {

  static final log.Logger _logger = new log.Logger('InMemoryCorpusRepository');

  /// Gets a Corpus by its name.
  @override
  Future<Corpus> getByName(String name) async {
    _logger.finer("Getting Corpus with name $name");
    return (await getAll()).firstWhere(
            (Corpus corpus) => corpus.name == name, orElse: () => null);
  }
}
