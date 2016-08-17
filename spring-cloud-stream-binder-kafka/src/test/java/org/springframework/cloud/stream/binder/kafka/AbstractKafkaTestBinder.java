package org.springframework.cloud.stream.binder.kafka;

import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;

import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.codec.kryo.KryoRegistrar;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.tuple.TupleKryoRegistrar;

/**
 * @author Soby Chacko
 */
public abstract class AbstractKafkaTestBinder extends
		AbstractTestBinder<KafkaMessageChannelBinder, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>> {

	@Override
	public void cleanup() {
		// do nothing - the rule will take care of that
	}

	protected static Codec getCodec() {
		return new PojoCodec(new TupleRegistrar());
	}

	private static class TupleRegistrar implements KryoRegistrar {
		private final TupleKryoRegistrar delegate = new TupleKryoRegistrar();

		@Override
		public void registerTypes(Kryo kryo) {
			this.delegate.registerTypes(kryo);
		}

		@Override
		public List<Registration> getRegistrations() {
			return this.delegate.getRegistrations();
		}
	}

}

