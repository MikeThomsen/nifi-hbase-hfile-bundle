package org.apache.nifi.processor.hbase;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "hbase", "record", "hfile", "bulk" })
@CapabilityDescription("This processor takes a record set and converts into HBase HFiles in HDFS, and when done, it notifies " +
    "HBase to import the newly generated HFiles into the main data set.")
public class PutHBaseHFileRecord extends AbstractHBaseHFileProcessor {
    public static final PropertyDescriptor READER = new PropertyDescriptor.Builder()
        .name("put-hbase-hfile-record-reader")
        .displayName("Record Reader")
        .description("A record reader service to use for reading an incoming record set and converting it into one or more HFiles.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .addValidator(Validator.VALID)
        .build();
    public static final PropertyDescriptor RECORDS_PER_HFILE = new PropertyDescriptor.Builder()
        .name("put-hbase-hfile-record-records-per-hfile")
        .displayName("Records Per HFile")
        .description("How many records to write to a HFile before starting a new one.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("10000")
        .required(true)
        .build();
    public static final PropertyDescriptor BASE_FOLDER = new PropertyDescriptor.Builder()
        .name("put-hbase-hfile-record-base-folder")
        .displayName("Base Folder")
        .description("The base folder in HDFS where the files will be written.")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(true)
        .build();
    public static final AllowableValue STRATEGY_STRINGIFY = new AllowableValue("stringify", "Stringify",
        "Turn complex records into a string.");
    public static final AllowableValue STRATEGY_IGNORE    = new AllowableValue("ignore", "Ignore", "Skip complex records.");
    public static final AllowableValue STRATEGY_ERROR     = new AllowableValue("error", "Raise Error", "Raise an error.");
    public static final PropertyDescriptor COMPLEX_RECORD_STRATEGY = new PropertyDescriptor.Builder()
        .name("put-hbase-hfile-record-complex-record-strategy")
        .displayName("Complex Record Strategy")
        .defaultValue(STRATEGY_STRINGIFY.getValue())
        .allowableValues(STRATEGY_STRINGIFY, STRATEGY_IGNORE, STRATEGY_ERROR)
        .description("The strategy to use for handling nested complex records.")
        .required(true)
        .addValidator(Validator.VALID)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
        .description("All successfully processed FlowFiles go to this relationship.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
        .description("All failed FlowFiles go to this relationship.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE
    )));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            READER, RECORDS_PER_HFILE, BASE_FOLDER, COMPLEX_RECORD_STRATEGY
    ));

    private volatile RecordReaderFactory readerFactory;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        try (InputStream is = session.read(input)) {
            RecordReader reader = readerFactory.createRecordReader(input, is, getLogger());

            reader.close();
            is.close();

            session.transfer(input, REL_SUCCESS);
            session.getProvenanceReporter().send(input, "Sent HFiles to HDFS cluster."); //TODO: Fix
        } catch (Exception ex) {
            getLogger().error("Error reading record set.", ex);
            session.transfer(input, REL_FAILURE);
        }
    }
}
