package com.wikia.nlp;

import com.wikia.nlp.collections.AWSBatchFileCollection;

import static edu.stanford.nlp.util.logging.Redwood.Util.endTrack;
import static edu.stanford.nlp.util.logging.Redwood.Util.err;
import static edu.stanford.nlp.util.logging.Redwood.Util.forceTrack;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.util.*;
import edu.stanford.nlp.util.logging.StanfordRedwoodConfiguration;

import edu.stanford.nlp.ie.NERClassifierCombiner;
import edu.stanford.nlp.ie.regexp.NumberSequenceClassifier;
import edu.stanford.nlp.ie.regexp.RegexNERSequenceClassifier;
import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.io.RuntimeIOException;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.trees.TreePrint;
import edu.stanford.nlp.util.logging.Redwood;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;


public class ParserDaemon extends StanfordCoreNLP{

	
	enum OutputFormat { TEXT, XML, SERIALIZED }
	
	// other constants
	public static final String CUSTOM_ANNOTATOR_PREFIX = "customAnnotatorClass.";
	private static final String PROPS_SUFFIX = ".properties";
	private static final String OUTPUT_DIR = "/tmp/xml";
	public static final String NEWLINE_SPLITTER_PROPERTY = "ssplit.eolonly";
	
	public static final String DEFAULT_OUTPUT_FORMAT = isXMLOutputPresent() ? "xml" : "text";
	
	/** Stores the overall number of words processed */
	private int numWords;
	/** Formats the constituent parse trees for display */
	private TreePrint constituentTreePrinter;
	/** Formats the dependency parse trees for human-readable display */
	private TreePrint dependencyTreePrinter;
	
	/** Maintains the shared pool of annotators */
	private static AnnotatorPool pool = null;
	
	private Properties properties;
	
	private static PropertiesCredentials awsprops;
	
    private static AmazonS3Client s3client;

	private static PropertiesCredentials getAwsProperties() throws IOException {
		if (awsprops != null) {
			awsprops = new PropertiesCredentials(
	                ParserDaemon.class.getResourceAsStream("AwsCredentials.properties")
			           );
		}
		return awsprops;
	}
	
	private static TransferManager getTransferManager() throws IOException {
		return new TransferManager(getAwsProperties());
	}
	
    private static AmazonS3Client getClient() throws IOException {
    	if (s3client != null) {
    		return s3client;
    	}
        AmazonS3Client client = new AmazonS3Client( new BasicAWSCredentials(
        												"AKIAJ6W4PSWS2FU7IK5A", 
        												"ZiIS6ujt+4c8I7IfmwyHccOgCkP9AFPLJsXICnc+"
														)
        										   );
        s3client = client;
        return client;
    }

    /**
     * Constructs a pipeline using as properties the properties file found in the classpath
     */
    public ParserDaemon() {
      this((Properties) null);
    }

    /**
     * Construct a basic pipeline. The Properties will be used to determine
     * which annotators to create, and a default AnnotatorPool will be used
     * to create the annotators.
     *
     */
    public ParserDaemon(Properties props)  {
      this(props, (props == null || PropertiesUtils.getBool(props, "enforceRequirements", true)));
    }

    public ParserDaemon(Properties props, boolean enforceRequirements)  {
      construct(props, enforceRequirements);
    }
    
    /**
     * Constructs a pipeline with the properties read from this file, which must be found in the classpath
     * @param propsFileNamePrefix
     */
    public ParserDaemon(String propsFileNamePrefix) {
      this(propsFileNamePrefix, true);
    }

    public ParserDaemon(String propsFileNamePrefix, boolean enforceRequirements) {
      Properties props = loadProperties(propsFileNamePrefix);
      if (props == null) {
        throw new RuntimeIOException("ERROR: cannot find properties file \"" + propsFileNamePrefix + "\" in the classpath!");
      }
      construct(props, enforceRequirements);
    }
    
    /**
     * Finds the properties file in the classpath and loads the properties from there.
     *
     * @return The found properties object (must be not-null)
     * @throws RuntimeException If no properties file can be found on the classpath
     */
    private static Properties loadPropertiesFromClasspath() {
      List<String> validNames = Arrays.asList("StanfordCoreNLP", "edu.stanford.nlp.pipeline.StanfordCoreNLP");
      for (String name: validNames) {
        Properties props = loadProperties(name);
        if (props != null) return props;
      }
      throw new RuntimeException("ERROR: Could not find properties file in the classpath!");
    }

    private static Properties loadProperties(String name) {
      return loadProperties(name, Thread.currentThread().getContextClassLoader());
    }

    private static Properties loadProperties(String name, ClassLoader loader){
      if(name.endsWith (PROPS_SUFFIX)) name = name.substring(0, name.length () - PROPS_SUFFIX.length ());
      name = name.replace('.', '/');
      name += PROPS_SUFFIX;
      Properties result = null;

      // Returns null on lookup failures
      System.err.println("Searching for resource: " + name);
      InputStream in = loader.getResourceAsStream (name);
      try {
        if (in != null) {
          InputStreamReader reader = new InputStreamReader(in, "utf-8");
          result = new Properties ();
          result.load(reader); // Can throw IOException
        }
      } catch (IOException e) {
        result = null;
      } finally {
        IOUtils.closeIgnoringExceptions(in);
      }

      return result;
    }

    private static String getRequiredProperty(Properties props, String name) {
        String val = props.getProperty(name);
        if (val == null) {
          System.err.println("Missing property \"" + name + "\"!");
          throw new RuntimeException("Missing property: \"" + name + '\"');
        }
        return val;
      }
    
    private void construct(Properties props, boolean enforceRequirements) {
        this.numWords = 0;
        this.constituentTreePrinter = new TreePrint("penn");
        this.dependencyTreePrinter = new TreePrint("typedDependenciesCollapsed");

        if (props == null) {
          // if undefined, find the properties file in the classpath
          props = loadPropertiesFromClasspath();
        } else if (props.getProperty("annotators") == null) {
          // this happens when some command line options are specified (e.g just "-filelist") but no properties file is.
          // we use the options that are given and let them override the default properties from the class path properties.
          Properties fromClassPath = loadPropertiesFromClasspath();
          fromClassPath.putAll(props);
          props = fromClassPath;
        }
        this.properties = props;
        AnnotatorPool pool = getDefaultAnnotatorPool(props);

        // now construct the annotators from the given properties in the given order
        List<String> annoNames = Arrays.asList(getRequiredProperty(props, "annotators").split("[, \t]+"));
        Set<String> alreadyAddedAnnoNames = Generics.newHashSet();
        Set<Requirement> requirementsSatisfied = Generics.newHashSet();
        for (String name : annoNames) {
          name = name.trim();
          if (name.isEmpty()) { continue; }
          System.err.println("Adding annotator " + name);

          Annotator an = pool.get(name);
          this.addAnnotator(an);

          if (enforceRequirements) {
            Set<Requirement> allRequirements = an.requires();
            for (Requirement requirement : allRequirements) {
              if (!requirementsSatisfied.contains(requirement)) {
                String fmt = "annotator \"%s\" requires annotator \"%s\"";
                throw new IllegalArgumentException(String.format(fmt, name, requirement));
              }
            }
            requirementsSatisfied.addAll(an.requirementsSatisfied());
          }

          // the NFL domain requires several post-processing rules after
          // tokenization.  add these transparently if the NFL annotator
          // is required
          if (name.equals(STANFORD_TOKENIZE) &&
              annoNames.contains(STANFORD_NFL) &&
              !annoNames.contains(STANFORD_NFL_TOKENIZE)) {
            Annotator pp = pool.get(STANFORD_NFL_TOKENIZE);
            this.addAnnotator(pp);
          }

          alreadyAddedAnnoNames.add(name);
        }

        // Sanity check
        if (! alreadyAddedAnnoNames.contains(STANFORD_SSPLIT)) {
          System.setProperty(NEWLINE_SPLITTER_PROPERTY, "false");
        }
      }
    
    /**
     * Prints the list of properties required to run the pipeline
     * @param os PrintStream to print usage to
     * @param helpTopic a topic to print help about (or null for general options)
     */
    private static void printHelp(PrintStream os, String helpTopic) {
      if (helpTopic.toLowerCase().startsWith("pars")) {
        os.println("StanfordCoreNLP currently supports the following parsers:");
        os.println("\tstanford - Stanford lexicalized parser (default)");
        os.println("\tcharniak - Charniak and Johnson reranking parser (sold separately)");
        os.println();
        os.println("General options: (all parsers)");
        os.println("\tparse.type - selects the parser to use");
        os.println("\tparse.model - path to model file for parser");
        os.println("\tparse.maxlen - maximum sentence length");
        os.println();
        os.println("Stanford Parser-specific options:");
        os.println("(In general, you shouldn't need to set this flags)");
        os.println("\tparse.flags - extra flags to the parser (default: -retainTmpSubcategories)");
        os.println("\tparse.debug - set to true to make the parser slightly more verbose");
        os.println();
        os.println("Charniak and Johnson parser-specific options:");
        os.println("\tparse.executable - path to the parseIt binary or parse.sh script");
      } else {
        // argsToProperties will set the value of a -h or -help to "true" if no arguments are given
        if ( ! helpTopic.equalsIgnoreCase("true")) {
          os.println("Unknown help topic: " + helpTopic);
          os.println("See -help for a list of all help topics.");
        } else {
          printRequiredProperties(os);
        }
      }
    }
    
    /**
     * Prints the list of properties required to run the pipeline
     * @param os PrintStream to print usage to
     */
    private static void printRequiredProperties(PrintStream os) {
      // TODO some annotators (ssplit, regexner, gender, some parser
      // options, dcoref?) are not documented
      os.println("The following properties can be defined:");
      os.println("(if -props or -annotators is not passed in, default properties will be loaded via the classpath)");
      os.println("\t\"props\" - path to file with configuration properties");
      os.println("\t\"annotators\" - comma separated list of annotators");
      os.println("\tThe following annotators are supported: cleanxml, tokenize, ssplit, pos, lemma, ner, truecase, parse, coref, dcoref, nfl");

      os.println("\n\tIf annotator \"tokenize\" is defined:");
      os.println("\t\"tokenize.options\" - PTBTokenizer options (see edu.stanford.nlp.process.PTBTokenizer for details)");
      os.println("\t\"tokenize.whitespace\" - If true, just use whitespace tokenization");

      os.println("\n\tIf annotator \"cleanxml\" is defined:");
      os.println("\t\"clean.xmltags\" - regex of tags to extract text from");
      os.println("\t\"clean.sentenceendingtags\" - regex of tags which mark sentence endings");
      os.println("\t\"clean.allowflawedxml\" - if set to false, don't complain about XML errors");

      os.println("\n\tIf annotator \"pos\" is defined:");
      os.println("\t\"pos.maxlen\" - maximum length of sentence to POS tag");
      os.println("\t\"pos.model\" - path towards the POS tagger model");

      os.println("\n\tIf annotator \"ner\" is defined:");
      os.println("\t\"ner.model.3class\" - path towards the three-class NER model");
      os.println("\t\"ner.model.7class\" - path towards the seven-class NER model");
      os.println("\t\"ner.model.MISCclass\" - path towards the NER model with a MISC class");

      os.println("\n\tIf annotator \"truecase\" is defined:");
      os.println("\t\"truecase.model\" - path towards the true-casing model; default: " + DefaultPaths.DEFAULT_TRUECASE_MODEL);
      os.println("\t\"truecase.bias\" - class bias of the true case model; default: " + TrueCaseAnnotator.DEFAULT_MODEL_BIAS);
      os.println("\t\"truecase.mixedcasefile\" - path towards the mixed case file; default: " + DefaultPaths.DEFAULT_TRUECASE_DISAMBIGUATION_LIST);

      os.println("\n\tIf annotator \"nfl\" is defined:");
      os.println("\t\"nfl.gazetteer\" - path towards the gazetteer for the NFL domain");
      os.println("\t\"nfl.relation.model\" - path towards the NFL relation extraction model");

      os.println("\n\tIf annotator \"parse\" is defined:");
      os.println("\t\"parse.model\" - path towards the PCFG parser model");

      /* XXX: unstable, do not use for now
      os.println("\n\tIf annotator \"srl\" is defined:");
      os.println("\t\"srl.verb.args\" - path to the file listing verbs and their core arguments (\"verbs.core_args\")");
      os.println("\t\"srl.model.id\" - path prefix for the role identification model (adds \".model.gz\" and \".fe\" to this prefix)");
      os.println("\t\"srl.model.cls\" - path prefix for the role classification model (adds \".model.gz\" and \".fe\" to this prefix)");
      os.println("\t\"srl.model.jic\" - path to the directory containing the joint model's \"model.gz\", \"fe\" and \"je\" files");
      os.println("\t                  (if not specified, the joint model will not be used)");
      */

      os.println("\nCommand line properties:");
      os.println("\t\"file\" - run the pipeline on the content of this file, or on the content of the files in this directory");
      os.println("\t         XML output is generated for every input file \"file\" as file.xml");
      os.println("\t\"extension\" - if -file used with a directory, process only the files with this extension");
      os.println("\t\"filelist\" - run the pipeline on the list of files given in this file");
      os.println("\t             output is generated for every input file as file.outputExtension");
      os.println("\t\"outputDirectory\" - where to put output (defaults to the current directory)");
      os.println("\t\"outputExtension\" - extension to use for the output file (defaults to \".xml\" for XML, \".ser.gz\" for serialized).  Don't forget the dot!");
      os.println("\t\"outputFormat\" - \"xml\" to output XML (default), \"serialized\" to output serialized Java objects, \"text\" to output text");
      os.println("\t\"replaceExtension\" - flag to chop off the last extension before adding outputExtension to file");
      os.println("\t\"noClobber\" - don't automatically override (clobber) output files that already exist");
  		os.println("\t\"threads\" - multithread on this number of threads");
      os.println("\nIf none of the above are present, run the pipeline in an interactive shell (default properties will be loaded from the classpath).");
      os.println("The shell accepts input from stdin and displays the output at stdout.");

      os.println("\nRun with -help [topic] for more help on a specific topic.");
      os.println("Current topics include: parser");

      os.println();
    }
    
    /**
     * Call this if you are no longer using StanfordCoreNLP and want to
     * release the memory associated with the annotators.
     */
    public static synchronized void clearAnnotatorPool() {
      pool = null;
    }

    private static synchronized AnnotatorPool getDefaultAnnotatorPool(final Properties inputProps) {
      // if the pool already exists reuse!
      if(pool == null) {
        // first time we get here
        pool = new AnnotatorPool();
      }

      //
      // tokenizer: breaks text into a sequence of tokens
      // this is required for all following annotators!
      //
      pool.register(STANFORD_TOKENIZE, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          if (Boolean.valueOf(properties.getProperty("tokenize.whitespace",
                            "false"))) {
            return new WhitespaceTokenizerAnnotator(properties);
          } else {
            String options = properties.getProperty("tokenize.options",
                    PTBTokenizerAnnotator.DEFAULT_OPTIONS);
            boolean keepNewline =
                    Boolean.valueOf(properties.getProperty(NEWLINE_SPLITTER_PROPERTY,
                            "false"));
            // If the user specifies "tokenizeNLs=false" in tokenize.options, then this default will
            // be overridden.
            if (keepNewline) {
              options = "tokenizeNLs," + options;
            }
            return new PTBTokenizerAnnotator(false, options);
          }
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          StringBuilder os = new StringBuilder();
          os.append("tokenize.whitespace:" +
                  properties.getProperty("tokenize.whitespace", "false"));
          if (Boolean.valueOf(properties.getProperty("tokenize.whitespace",
                  "false"))) {
            os.append(WhitespaceTokenizerAnnotator.EOL_PROPERTY + ":" +
                    properties.getProperty(WhitespaceTokenizerAnnotator.EOL_PROPERTY,
                            "false"));
            os.append(StanfordCoreNLP.NEWLINE_SPLITTER_PROPERTY + ":" +
                    properties.getProperty(StanfordCoreNLP.NEWLINE_SPLITTER_PROPERTY,
                            "false"));
            return os.toString();
          } else {
            os.append(NEWLINE_SPLITTER_PROPERTY + ":" +
                    Boolean.valueOf(properties.getProperty(NEWLINE_SPLITTER_PROPERTY,
                            "false")));
          }
          return os.toString();
        }
      });

      pool.register(STANFORD_CLEAN_XML, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          String xmlTags =
            properties.getProperty("clean.xmltags",
                              CleanXmlAnnotator.DEFAULT_XML_TAGS);
          String sentenceEndingTags =
            properties.getProperty("clean.sentenceendingtags",
                              CleanXmlAnnotator.DEFAULT_SENTENCE_ENDERS);
          String allowFlawedString = properties.getProperty("clean.allowflawedxml");
          boolean allowFlawed = CleanXmlAnnotator.DEFAULT_ALLOW_FLAWS;
          if (allowFlawedString != null)
            allowFlawed = Boolean.valueOf(allowFlawedString);
          String dateTags =
            properties.getProperty("clean.datetags",
                              CleanXmlAnnotator.DEFAULT_DATE_TAGS);
          return new CleanXmlAnnotator(xmlTags,
              sentenceEndingTags,
              dateTags,
              allowFlawed);
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          return "clean.xmltags:" +
                  properties.getProperty("clean.xmltags",
                  CleanXmlAnnotator.DEFAULT_XML_TAGS) +
                  "clean.sentenceendingtags:" +
                  properties.getProperty("clean.sentenceendingtags",
                  CleanXmlAnnotator.DEFAULT_SENTENCE_ENDERS) +
                  "clean.allowflawedxml:" +
                  properties.getProperty("clean.allowflawedxml", "") +
                  "clean.datetags:" +
                  properties.getProperty("clean.datetags",
                  CleanXmlAnnotator.DEFAULT_DATE_TAGS);
        }
      });

      //
      // sentence splitter: splits the above sequence of tokens into
      // sentences.  This is required when processing entire documents or
      // text consisting of multiple sentences
      //
      pool.register(STANFORD_SSPLIT, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          boolean nlSplitting = Boolean.valueOf(properties.getProperty(NEWLINE_SPLITTER_PROPERTY, "false"));
          if (nlSplitting) {
            boolean whitespaceTokenization = Boolean.valueOf(properties.getProperty("tokenize.whitespace", "false"));
            WordsToSentencesAnnotator wts;
            if (whitespaceTokenization) {
              if (System.getProperty("line.separator").equals("\n")) {
                wts = WordsToSentencesAnnotator.newlineSplitter(false, "\n");
              } else {
                // throw "\n" in just in case files use that instead of
                // the system separator
                wts = WordsToSentencesAnnotator.newlineSplitter(false, System.getProperty("line.separator"), "\n");
              }
            } else {
              wts = WordsToSentencesAnnotator.newlineSplitter(false, PTBTokenizer.getNewlineToken());
            }

            wts.setCountLineNumbers(true);

            return wts;
          } else {
            WordsToSentencesAnnotator wts;
            String boundaryTokenRegex = properties.getProperty("ssplit.boundaryTokenRegex");
            if (boundaryTokenRegex != null) {
              wts = new WordsToSentencesAnnotator(false, boundaryTokenRegex);
            } else {
              wts = new WordsToSentencesAnnotator();
            }

            // regular boundaries
            String bounds = properties.getProperty("ssplit.boundariesToDiscard");
            if (bounds != null){
              String [] toks = bounds.split(",");
              // for(int i = 0; i < toks.length; i ++)
              //   System.err.println("BOUNDARY: " + toks[i]);
              wts.setSentenceBoundaryToDiscard(Generics.newHashSet (Arrays.asList(toks)));
            }

            // HTML boundaries
            bounds = properties.getProperty("ssplit.htmlBoundariesToDiscard");
            if (bounds != null){
              String [] toks = bounds.split(",");
              wts.addHtmlSentenceBoundaryToDiscard(Generics.newHashSet (Arrays.asList(toks)));
            }

            // Treat as one sentence
            String isOneSentence = properties.getProperty("ssplit.isOneSentence");
            if (isOneSentence != null){
              wts.setOneSentence(Boolean.parseBoolean(isOneSentence));
            }

            return wts;
          }
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          StringBuilder os = new StringBuilder();
          os.append(NEWLINE_SPLITTER_PROPERTY + ":" +
                  properties.getProperty(NEWLINE_SPLITTER_PROPERTY, "false"));
          if(Boolean.valueOf(properties.getProperty(NEWLINE_SPLITTER_PROPERTY,
                  "false"))) {
            os.append("tokenize.whitespace:" +
                    properties.getProperty("tokenize.whitespace", "false"));
          } else {
            os.append("ssplit.boundariesToDiscard:" +
                    properties.getProperty("ssplit.boundariesToDiscard", ""));
            os.append("ssplit.htmlBoundariesToDiscard:" +
                    properties.getProperty("ssplit.htmlBoundariesToDiscard", ""));
            os.append("ssplit.isOneSentence:" +
                    properties.getProperty("ssplit.isOneSentence", ""));
          }
          return os.toString();
        }
      });

      //
      // POS tagger
      //
      pool.register(STANFORD_POS, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          try {
            return new POSTaggerAnnotator("pos", properties);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          return ("pos.maxlen:" + properties.getProperty("pos.maxlen", "") +
                  "pos.model:" + properties.getProperty("pos.model", DefaultPaths.DEFAULT_POS_MODEL) +
                  "pos.nthreads:" + properties.getProperty("pos.nthreads", properties.getProperty("nthreads", "")));
        }
      });

      //
      // Lemmatizer
      //
      pool.register(STANFORD_LEMMA, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          return new MorphaAnnotator(false);
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          // nothing for this one
          return "";
        }
      });

      //
      // NER
      //
      pool.register(STANFORD_NER, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          List<String> models = new ArrayList<String>();
          String modelNames = properties.getProperty("ner.model");
          if (modelNames == null) {
            modelNames = DefaultPaths.DEFAULT_NER_THREECLASS_MODEL + "," + DefaultPaths.DEFAULT_NER_MUC_MODEL + "," + DefaultPaths.DEFAULT_NER_CONLL_MODEL;
          }
          if (modelNames.length() > 0) {
            models.addAll(Arrays.asList(modelNames.split(",")));
          }
          if (models.isEmpty()) {
            // Allow for no real NER model - can just use numeric classifiers or SUTime
            // Will have to explicitly unset ner.model.3class, ner.model.7class, ner.model.MISCclass
            // So unlikely that people got here by accident
            System.err.println("WARNING: no NER models specified");
          }
          NERClassifierCombiner nerCombiner;
          try {
            boolean applyNumericClassifiers =
              PropertiesUtils.getBool(properties,
                  NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_PROPERTY,
                  NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_DEFAULT);
            boolean useSUTime =
              PropertiesUtils.getBool(properties,
                  NumberSequenceClassifier.USE_SUTIME_PROPERTY,
                  NumberSequenceClassifier.USE_SUTIME_DEFAULT);
            nerCombiner = new NERClassifierCombiner(applyNumericClassifiers,
                  useSUTime, properties,
                  models.toArray(new String[models.size()]));
          } catch (FileNotFoundException e) {
            throw new RuntimeIOException(e);
          }
          return new NERCombinerAnnotator(nerCombiner, false);
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          return "ner.model:" +
                  properties.getProperty("ner.model", "") +
                  "ner.model.3class:" +
                  properties.getProperty("ner.model.3class",
                          DefaultPaths.DEFAULT_NER_THREECLASS_MODEL) +
                  "ner.model.7class:" +
                  properties.getProperty("ner.model.7class",
                          DefaultPaths.DEFAULT_NER_MUC_MODEL) +
                  "ner.model.MISCclass:" +
                  properties.getProperty("ner.model.MISCclass",
                          DefaultPaths.DEFAULT_NER_CONLL_MODEL) +
                  NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_PROPERTY + ":" +
                  properties.getProperty(NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_PROPERTY,
                          Boolean.toString(NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_DEFAULT)) +
                  NumberSequenceClassifier.USE_SUTIME_PROPERTY + ":" +
                  properties.getProperty(NumberSequenceClassifier.USE_SUTIME_PROPERTY,
                          Boolean.toString(NumberSequenceClassifier.USE_SUTIME_DEFAULT));
        }
      });

      //
      // Regex NER
      //
      pool.register(STANFORD_REGEXNER, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          String mapping = properties.getProperty("regexner.mapping", DefaultPaths.DEFAULT_REGEXNER_RULES);
          String ignoreCase = properties.getProperty("regexner.ignorecase", "false");
          String validPosPattern = properties.getProperty("regexner.validpospattern", RegexNERSequenceClassifier.DEFAULT_VALID_POS);
          return new RegexNERAnnotator(mapping, Boolean.valueOf(ignoreCase), validPosPattern);
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          return "regexner.mapping:" +
                  properties.getProperty("regexner.mapping",
                          DefaultPaths.DEFAULT_REGEXNER_RULES) +
                  "regexner.ignorecase:" +
                  properties.getProperty("regexner.ignorecase",
                          "false") +
                  "regexner.validpospattern:" +
                  properties.getProperty("regexner.validpospattern",
                          RegexNERSequenceClassifier.DEFAULT_VALID_POS);
        }
      });

      //
      // Gender Annotator
      //
      pool.register(STANFORD_GENDER, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          return new GenderAnnotator(false, properties.getProperty("gender.firstnames", DefaultPaths.DEFAULT_GENDER_FIRST_NAMES));
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          return "gender.firstnames:" +
                  properties.getProperty("gender.firstnames",
                          DefaultPaths.DEFAULT_GENDER_FIRST_NAMES);
        }
      });


      //
      // True caser
      //
      pool.register(STANFORD_TRUECASE, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          String model = properties.getProperty("truecase.model", DefaultPaths.DEFAULT_TRUECASE_MODEL);
          String bias = properties.getProperty("truecase.bias", TrueCaseAnnotator.DEFAULT_MODEL_BIAS);
          String mixed = properties.getProperty("truecase.mixedcasefile", DefaultPaths.DEFAULT_TRUECASE_DISAMBIGUATION_LIST);
          return new TrueCaseAnnotator(model, bias, mixed, false);
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          return "truecase.model:" +
                  properties.getProperty("truecase.model",
                          DefaultPaths.DEFAULT_TRUECASE_MODEL) +
                  "truecase.bias:" +
                  properties.getProperty("truecase.bias",
                          TrueCaseAnnotator.DEFAULT_MODEL_BIAS) +
                  "truecase.mixedcasefile:" +
                  properties.getProperty("truecase.mixedcasefile",
                          DefaultPaths.DEFAULT_TRUECASE_DISAMBIGUATION_LIST);
        }
      });

      //
      // Post-processing tokenization rules for the NFL domain
      //
      pool.register(STANFORD_NFL_TOKENIZE, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          final String className =
            "edu.stanford.nlp.pipeline.NFLTokenizerAnnotator";
          return ReflectionLoading.loadByReflection(className);
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          // no used props for this one
          return "";
        }
      });

      //
      // Entity and relation extraction for the NFL domain
      //
      pool.register(STANFORD_NFL, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          // these paths now extracted inside c'tor
          // String gazetteer = properties.getProperty("nfl.gazetteer", DefaultPaths.DEFAULT_NFL_GAZETTEER);
          // String entityModel = properties.getProperty("nfl.entity.model", DefaultPaths.DEFAULT_NFL_ENTITY_MODEL);
          // String relationModel = properties.getProperty("nfl.relation.model", DefaultPaths.DEFAULT_NFL_RELATION_MODEL);
          final String className = "edu.stanford.nlp.pipeline.NFLAnnotator";
          return ReflectionLoading.loadByReflection(className, properties);
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          return "nfl.verbose:" +
                  properties.getProperty("nfl.verbose",
                          "false") +
                  "nfl.relations.use.max.recall:" +
                  properties.getProperty("nfl.relations.use.max.recall",
                          "false") +
                  "nfl.relations.use.model.merging:" +
                  properties.getProperty("nfl.relations.use.model.merging",
                          "false") +
                  "nfl.relations.use.basic.inference:" +
                  properties.getProperty("nfl.relations.use.basic.inference",
                          "true") +
                  "nfl.gazetteer:" +
                  properties.getProperty("nfl.gazetteer",
                          DefaultPaths.DEFAULT_NFL_GAZETTEER) +
                  "nfl.entity.model:" +
                  properties.getProperty("nfl.entity.model",
                          DefaultPaths.DEFAULT_NFL_ENTITY_MODEL) +
                  "nfl.relation.model:" +
                  properties.getProperty("nfl.relation.model",
                          DefaultPaths.DEFAULT_NFL_RELATION_MODEL);
        }
      });

      //
      // Parser
      //
      pool.register(STANFORD_PARSE, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          String parserType = properties.getProperty("parse.type", "stanford");
          String maxLenStr = properties.getProperty("parse.maxlen");

          if (parserType.equalsIgnoreCase("stanford")) {
            ParserAnnotator anno = new ParserAnnotator("parse", properties);
            return anno;
          } else if (parserType.equalsIgnoreCase("charniak")) {
            String model = properties.getProperty("parse.model");
            String parserExecutable = properties.getProperty("parse.executable");
            if (model == null || parserExecutable == null) {
              throw new RuntimeException("Both parse.model and parse.executable properties must be specified if parse.type=charniak");
            }
            int maxLen = 399;
            if (maxLenStr != null) {
              maxLen = Integer.parseInt(maxLenStr);
            }

            CharniakParserAnnotator anno = new CharniakParserAnnotator(model, parserExecutable, false, maxLen);

            return anno;
          } else {
            throw new RuntimeException("Unknown parser type: " + parserType + " (currently supported: stanford and charniak)");
          }
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          String type = properties.getProperty("parse.type", "stanford");
          if(type.equalsIgnoreCase("stanford")){
            return ParserAnnotator.signature("parser", properties);
          } else if(type.equalsIgnoreCase("charniak")) {
            return "parse.model:" +
                    properties.getProperty("parse.model", "") +
                    "parse.executable:" +
                    properties.getProperty("parse.executable", "") +
                    "parse.maxlen:" +
                    properties.getProperty("parse.maxlen", "");
          } else {
            throw new RuntimeException("Unknown parser type: " + type +
                    " (currently supported: stanford and charniak)");
          }
        }
      });

      //
      // Coreference resolution
      //
      pool.register(STANFORD_DETERMINISTIC_COREF, new AnnotatorFactory(inputProps) {
        private static final long serialVersionUID = 1L;
        @Override
        public Annotator create() {
          return new DeterministicCorefAnnotator(properties);
        }

        @Override
        public String signature() {
          // keep track of all relevant properties for this annotator here!
          return DeterministicCorefAnnotator.signature(properties);
        }
      });

      // add annotators loaded via reflection from classnames specified
      // in the properties
      for (Object propertyKey : inputProps.keySet()) {
        if (!(propertyKey instanceof String))
          continue; // should this be an Exception?
        String property = (String) propertyKey;
        if (property.startsWith(CUSTOM_ANNOTATOR_PREFIX)) {
          final String customName =
            property.substring(CUSTOM_ANNOTATOR_PREFIX.length());
          final String customClassName = inputProps.getProperty(property);
          System.err.println("Registering annotator " + customName +
              " with class " + customClassName);
          pool.register(customName, new AnnotatorFactory(inputProps) {
            private static final long serialVersionUID = 1L;
            private final String name = customName;
            private final String className = customClassName;
            @Override
            public Annotator create() {
              return ReflectionLoading.loadByReflection(className, name,
                                                        properties);
            }
            @Override
            public String signature() {
              // keep track of all relevant properties for this annotator here!
              // since we don't know what props they need, let's copy all
              // TODO: can we do better here? maybe signature() should be a method in the Annotator?
              StringBuilder os = new StringBuilder();
              for(Object key: properties.keySet()) {
                String skey = (String) key;
                os.append(skey + ":" + properties.getProperty(skey));
              }
              return os.toString();
            }
          });
        }
      }


      //
      // add more annotators here!
      //
      return pool;
    }

    public static synchronized Annotator getExistingAnnotator(String name) {
      if(pool == null){
        System.err.println("ERROR: attempted to fetch annotator \"" + name + "\" before the annotator pool was created!");
        return null;
      }
      try {
        Annotator a =  pool.get(name);
        return a;
      } catch(IllegalArgumentException e) {
        System.err.println("ERROR: attempted to fetch annotator \"" + name + "\" but the annotator pool does not store any such type!");
        return null;
      }
    }
    
	/**
	   * This can be used just for testing or for command-line text processing.
	   * This runs the pipeline you specify on the
	   * text in the file that you specify and sends some results to stdout.
	   * The current code in this main method assumes that each line of the file
	   * is to be processed separately as a single sentence.
	   * <p>
	   * Example usage:<br>
	   * java -mx6g edu.stanford.nlp.pipeline.StanfordCoreNLP properties
	   *
	   * @param args List of required properties
	   * @throws java.io.IOException If IO problem
	   * @throws ClassNotFoundException If class loading problem
	   */
	  public static void main(String[] args) throws IOException, ClassNotFoundException {
		  
		    StanfordRedwoodConfiguration.minimalSetup();
		    //
		    // process the arguments
		    //
		    // extract all the properties from the command line
		    // if cmd line is empty, set the properties to null. The processor will search for the properties file in the classpath
		    Properties props = null;
		    if (args.length > 0) {
		      props = StringUtils.argsToProperties(args);
		      boolean hasH = props.containsKey("h");
		      boolean hasHelp = props.containsKey("help");
		      if (hasH || hasHelp) {
		        String helpValue = hasH ? props.getProperty("h") : props.getProperty("help");
		        printHelp(System.err, helpValue);
		        return;
		      }
		    }
		    // multithreading thread count
		    String numThreadsString = (props == null) ? null : props.getProperty("threads");
		    int numThreads = 1;
		    try{
		      if (numThreadsString != null) {
		        numThreads = Integer.parseInt(numThreadsString);
		      }
		    } catch(NumberFormatException e) {
		      err("-threads [number]: was not given a valid number: " + numThreadsString);
		    }

		    ParserDaemon pipeline = new ParserDaemon(props);
		    
		    //
		    // construct the pipeline
		    //
		    
		    Collection<String> registered = new ArrayList<String>();
		    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
	        final ReentrantLock metaInfoLock = new ReentrantLock();
	        Redwood.Util.startThreads("ParserDaemon");
		    while (true) {
		    	Collection<File> filesInDir = Arrays.asList(new File("/tmp/text").listFiles());
		    	Collection<File> filesToParse = new ArrayList<File>();
		    	for (File fl: filesInDir) {
		    		String name = fl.getName();
		    		if (!registered.contains(name)) {
		    			registered.add(name);
		    			filesToParse.add(fl);
		    		}
		    	}
		    	
		    	int count = filesToParse.size();
		    	if (count > 0) {
		    		pipeline.processFiles(filesToParse, exec, metaInfoLock);
		    	}
		    }
	  }
	  
    /**
     * Overriding parent to make iterability easier
     */
    public void processFiles(final Collection<File> files, ExecutorService exec, final ReentrantLock metaInfoLock) throws IOException {
        File outputdir = new File(OUTPUT_DIR);
        if (! outputdir.exists()) {
        	outputdir.mkdir();
        }
        
        for (final File file : files) {
          //register a task...
          final Runnable runme = (new Runnable(){
            //who's run() method is...
            @Override
            public void run(){
              //catching exceptions...
              try {
            	  //--Get Output File Info
                  //(filename)
            	  if (! file.exists() ) {
            		  System.out.println(file.getName()+" already processed");
            		  return;
            	  }
                  String outputFilename = new File(OUTPUT_DIR, file.getName()).getPath();
                  if (properties.getProperty("replaceExtension") != null) {
                    int lastDot = outputFilename.lastIndexOf('.');
                    // for paths like "./zzz", lastDot will be 0
                    if (lastDot > 0) {
                      outputFilename = outputFilename.substring(0, lastDot);
                    }
                  }
                  //(file info)
                  String extension = ".xml";
                  // ensure we don't make filenames with doubled extensions like .xml.xml
                  if (!outputFilename.endsWith(extension)) {
                    outputFilename += extension;
                  }
                  // normalize filename for the upcoming comparison
                  outputFilename = new File(outputFilename).getCanonicalPath();

                  //--Conditions For Skipping The File
                  if (outputFilename.equals(file.getCanonicalPath())) {
                    err("Skipping " + file.getName() + ": output file " + outputFilename + " has the same filename as the input file -- assuming you don't actually want to do this.");
                    return;
                  }

                  //--Process File
                  Annotation annotation = null;
                  //(read file)
                  if (annotation == null) {
                	  String encoding = getEncoding();
                	  String text = IOUtils.slurpFile(file, encoding);
                	  annotation = new Annotation(text);
                  }

                  annotate(annotation);

                  
                  String fname = file.getName();
                  forceTrack("Processing file " + file.getAbsolutePath() + " ... writing to /tmp/xml/"+fname+".xml");
                
                  FileOutputStream fos = new FileOutputStream(outputFilename);
                  OutputStream bfos = new BufferedOutputStream(fos);
                  xmlPrint(annotation, bfos);
                  fos.close();
                  bfos.close();
                  file.delete();

                  endTrack("Processing file " + file.getAbsolutePath() + " ... writing to /tmp/xml/"+fname+".xml");
              } catch (IOException e) {
                throw new RuntimeIOException(e);
              }
            }
          });
          
          exec.submit(new Runnable(){
	          public void run(){
	            try{
	              //(signal start of threads)
	              metaInfoLock.lock();
	              metaInfoLock.unlock();
	              //(run runnable)
	              try{
	                runme.run();
	              } catch (Exception e){
	                e.printStackTrace();
	                System.exit(1);
	              } catch (AssertionError e) {
	                e.printStackTrace();
	                System.exit(1);
	              }
	              //(signal end of thread)
	              Redwood.Util.finishThread();
	              //(signal end of threads)
	            } catch(Throwable t){
	              t.printStackTrace();
	              System.exit(1);
	            }
	          }
	        });
          
        }
      }
}
