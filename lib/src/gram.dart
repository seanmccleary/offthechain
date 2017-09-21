import 'dart:math';
import 'package:dddart/dddart.dart';
import 'package:logging/logging.dart' as log;
import 'gram_item.dart';

/// A sequence of items in a markov chain
class Gram<T extends GramItem> extends AggregateRoot {

  static final log.Logger _logger = new log.Logger('Gram');
  static final Random _random = new Random();

  /// The ID of the Corpus to which this [Gram] belongs
  String corpusId;

  /// The number of times this gram occurs within the Corpus
  int occurrences;

  /// The items which make up this gram
  final List<T> _items = new List<T>();

  /// Whether or not this [Gram] represents the start of a Markov chain
  bool isStartOfChain;

  /// Whether or not this [Gram] represents the end of a Markov chain
  bool isEndOfChain;

  /// Instantiates a [Gram]
  Gram(this.corpusId, this.occurrences, {String id = null, DateTime created,
    DateTime updated, this.isStartOfChain = false, this.isEndOfChain = false})
      : super(id: id, created: created, updated: updated);

  /// Adds an item to this [Gram]
  void addItem(T item) {
    _items.add(item);
  }

  /// The items which make up this [Gram]
  List<T> get items => new List<T>.unmodifiable(_items);

  /// Tests whether or not the given items match the items in this Gram's sequence.
  ///
  /// You may need to override == on your objects to make this behave as desired.
  bool areItemsEqual(List<T> comparisonItems) => _areItemSequencesEqual(items, comparisonItems);

  /// Tests whether the given items match the items at the beginning of this Gram's sequence.
  ///
  /// You may need to override == on your objects to make this behave as desired.
  bool areItemsPrefix(List<T> comparisonItems) {

    if (items.length <= comparisonItems.length) {
      return false;
    }

    return _areItemSequencesEqual(items.getRange(0, comparisonItems.length),
        comparisonItems);
  }

  /// Tests two sequences of objects for equality.
  ///
  /// You might need to override the == operator to get this to behave as desired.
  /// This is necessary because the list equality functionality provided by the
  /// Dart collections package doesn't play nicely with Generics.
  static bool _areItemSequencesEqual(Iterable<Object> items1, Iterable<Object> items2) {
    if (items1.length != items2.length) {
      return false;
    }

    for (int count = 0; count < items1.length; count++) {
      if (items1.elementAt(count) != items2.elementAt(count)) {
        return false;
      }
    }
    return true;

  }

  /// Selects a gram from a collection based on its probability of occurring
  static Gram<T> selectGramByProbability<T extends GramItem>(List<Gram<T>> grams) {

    _logger.finer("Going to calculate probability for ${grams.length} grams");
    int totalOccurrences = 0;
    grams.forEach((Gram<T> gram) => totalOccurrences += gram.occurrences);
    final int occurrenceToStopOn = _random.nextInt(totalOccurrences) + 1;
    int occurrenceCounter = 0;

    for (Gram<T> gram in grams) {
      occurrenceCounter += gram.occurrences;
      if (occurrenceCounter >= occurrenceToStopOn) {
        return gram;
      }
    }

    // How'd we get here?
    return null;
  }

}
