package com.redis.multidc.config;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SSL certificate configuration in DatacenterEndpoint.
 */
public class DatacenterEndpointSslConfigTest {

    @Test
    public void testBasicSslConfiguration() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
                .id("ssl-test")
                .host("redis.example.com")
                .port(6380)
                .ssl(true)
                .build();
        
        assertTrue(endpoint.isSsl());
        assertFalse(endpoint.hasSslCertificateConfig());
        assertFalse(endpoint.isMutualTlsEnabled());
    }

    @Test
    public void testSslWithTruststoreConfiguration() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
                .id("ssl-truststore-test")
                .host("redis.example.com")
                .port(6380)
                .ssl(true)
                .truststorePath("/path/to/truststore.jks")
                .truststorePassword("truststore-password")
                .truststoreType("JKS")
                .verifyHostname(true)
                .verifyPeer(true)
                .build();
        
        assertTrue(endpoint.isSsl());
        assertTrue(endpoint.hasSslCertificateConfig());
        assertFalse(endpoint.isMutualTlsEnabled());
        assertEquals("/path/to/truststore.jks", endpoint.getTruststorePath());
        assertEquals("truststore-password", endpoint.getTruststorePassword());
        assertEquals("JKS", endpoint.getTruststoreType());
        assertTrue(endpoint.isVerifyHostname());
        assertTrue(endpoint.isVerifyPeer());
    }

    @Test
    public void testMutualTlsConfiguration() {
        List<String> cipherSuites = Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256");
        List<String> protocols = Arrays.asList("TLSv1.3", "TLSv1.2");
        
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
                .id("mtls-test")
                .host("redis.example.com")
                .port(6380)
                .ssl(true)
                .truststorePath("/path/to/truststore.jks")
                .truststorePassword("truststore-password")
                .keystorePath("/path/to/keystore.jks")
                .keystorePassword("keystore-password")
                .clientCertificatePath("/path/to/client.crt")
                .clientPrivateKeyPath("/path/to/client.key")
                .cipherSuites(cipherSuites)
                .protocols(protocols)
                .verifyHostname(true)
                .verifyPeer(true)
                .build();
        
        assertTrue(endpoint.isSsl());
        assertTrue(endpoint.hasSslCertificateConfig());
        assertTrue(endpoint.isMutualTlsEnabled());
        
        assertEquals("/path/to/truststore.jks", endpoint.getTruststorePath());
        assertEquals("truststore-password", endpoint.getTruststorePassword());
        assertEquals("/path/to/keystore.jks", endpoint.getKeystorePath());
        assertEquals("keystore-password", endpoint.getKeystorePassword());
        assertEquals("/path/to/client.crt", endpoint.getClientCertificatePath());
        assertEquals("/path/to/client.key", endpoint.getClientPrivateKeyPath());
        assertEquals(cipherSuites, endpoint.getCipherSuites());
        assertEquals(protocols, endpoint.getProtocols());
    }

    @Test
    public void testSslWithDefaultsConvenienceMethod() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
                .id("ssl-defaults-test")
                .host("redis.example.com")
                .port(6380)
                .enableSslWithDefaults()
                .build();
        
        assertTrue(endpoint.isSsl());
        assertTrue(endpoint.isVerifyHostname());
        assertTrue(endpoint.isVerifyPeer());
    }

    @Test
    public void testMutualTlsConvenienceMethod() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
                .id("mtls-convenience-test")
                .host("redis.example.com")
                .port(6380)
                .enableMutualTls("/path/to/client.crt", "/path/to/client.key", "/path/to/truststore.jks", "truststore-password")
                .build();
        
        assertTrue(endpoint.isSsl());
        assertTrue(endpoint.isMutualTlsEnabled());
        assertTrue(endpoint.isVerifyHostname());
        assertTrue(endpoint.isVerifyPeer());
        assertEquals("/path/to/client.crt", endpoint.getClientCertificatePath());
        assertEquals("/path/to/client.key", endpoint.getClientPrivateKeyPath());
        assertEquals("/path/to/truststore.jks", endpoint.getTruststorePath());
        assertEquals("truststore-password", endpoint.getTruststorePassword());
    }

    @Test
    public void testMutualTlsWithoutPasswordConvenienceMethod() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
                .id("mtls-no-password-test")
                .host("redis.example.com")
                .port(6380)
                .enableMutualTlsWithoutPassword("/path/to/client.crt", "/path/to/client.key")
                .build();
        
        assertTrue(endpoint.isSsl());
        assertTrue(endpoint.isMutualTlsEnabled());
        assertTrue(endpoint.isVerifyHostname());
        assertTrue(endpoint.isVerifyPeer());
        assertEquals("/path/to/client.crt", endpoint.getClientCertificatePath());
        assertEquals("/path/to/client.key", endpoint.getClientPrivateKeyPath());
        assertNull(endpoint.getTruststorePath());
        assertNull(endpoint.getTruststorePassword());
    }

    @Test
    public void testSslValidationRequiresMutualKeyAndCert() {
        assertThrows(IllegalArgumentException.class, () -> {
            DatacenterEndpoint.builder()
                    .id("invalid-ssl-test")
                    .host("redis.example.com")
                    .port(6380)
                    .ssl(true)
                    .clientCertificatePath("/path/to/client.crt")
                    // Missing clientPrivateKeyPath
                    .build();
        });
    }

    @Test
    public void testSslValidationRequiresTruststorePassword() {
        assertThrows(IllegalArgumentException.class, () -> {
            DatacenterEndpoint.builder()
                    .id("invalid-truststore-test")
                    .host("redis.example.com")
                    .port(6380)
                    .ssl(true)
                    .truststorePath("/path/to/truststore.jks")
                    // Missing truststorePassword
                    .build();
        });
    }

    @Test
    public void testSslValidationRequiresKeystorePassword() {
        assertThrows(IllegalArgumentException.class, () -> {
            DatacenterEndpoint.builder()
                    .id("invalid-keystore-test")
                    .host("redis.example.com")
                    .port(6380)
                    .ssl(true)
                    .keystorePath("/path/to/keystore.jks")
                    // Missing keystorePassword
                    .build();
        });
    }

    @Test
    public void testSslWithEmptyProtocolsAndCipherSuites() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
                .id("ssl-empty-lists-test")
                .host("redis.example.com")
                .port(6380)
                .ssl(true)
                .cipherSuites(Arrays.asList())
                .protocols(Arrays.asList())
                .build();
        
        assertTrue(endpoint.isSsl());
        assertTrue(endpoint.getCipherSuites().isEmpty());
        assertTrue(endpoint.getProtocols().isEmpty());
    }

    @Test
    public void testSslConnectionString() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
                .id("ssl-connection-test")
                .host("redis.example.com")
                .port(6380)
                .ssl(true)
                .password("secret")
                .database(1)
                .build();
        
        String connectionString = endpoint.getConnectionString();
        assertTrue(connectionString.startsWith("rediss://"));
        assertTrue(connectionString.contains(":secret@"));
        assertTrue(connectionString.contains("redis.example.com:6380/1"));
    }
}
