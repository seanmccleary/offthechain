import 'dart:async';
import 'dart:math';
import 'package:dddart/dddart.dart';
import 'package:logging/logging.dart' as log;
import '../../gram.dart';
import '../../gram_item.dart';
import '../gram_repository.dart';

/// An in-memory implementation of the Gram repository
class InMemoryGramRepository<T extends GramItem>
    extends InMemoryAggregateRootRepository<Gram<T>>
    implements GramRepository<T> {

  static final log.Logger _logger = new log.Logger('InMemoryGramRepository');
  final Random _random = new Random();

  @override
  Future<Gram<T>> getByItems(List<T> items, String corpusId) async {

    _logger.finer("Getting Grams with items ${items.join(" ")}");

    final List<Gram<T>> grams = (await getAll()).where(
            (Gram<T> gram) => corpusId == gram.corpusId).toList();

    for (Gram<T> gram in grams) {

      if (gram.areItemsEqual(items)) {
        return gram;
      }
    }

    return null;
  }

  @override
  Future<Gram<T>> getByTextPrefix(List<T> items, List<String> corpusIds) async {
    _logger.finer("Getting grams by text prefix ${items.join(" " )}");

    final List<Gram<T>> grams = (await getAll()).where(
            (Gram<T> gram) => corpusIds.contains(gram.corpusId)).toList();

    final List<Gram<T>> matchingGrams = new List<Gram<T>>();

    for (Gram<T> gram in grams) {

      if (gram.areItemsPrefix(items)) {
        matchingGrams.add(gram);
      }
    }

    return Gram.selectGramByProbability<T>(matchingGrams);
  }

  @override
  Future<Gram<T>> getRandomStartingGram(List<String> corpusIds) async {
    _logger.finer("Getting a random starting Gram for Corpuses ${corpusIds.join(" ")}");
    final List<Gram<T>> grams = (await getAll())
        .where((Gram<T> gram) => gram.isStartOfChain && corpusIds.contains(gram.corpusId))
        .toList();
    return grams[_random.nextInt(grams.length)];
  }
}