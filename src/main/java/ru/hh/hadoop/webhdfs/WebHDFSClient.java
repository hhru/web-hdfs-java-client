package ru.hh.hadoop.webhdfs;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import java.net.URI;
import java.util.Map;

public class WebHDFSClient {

  private final WebResource webHDFSResource;
  private final Client client;

  public WebHDFSClient(String baseUrl) {
    this(baseUrl, null);
  }

  public WebHDFSClient(String baseUrl, Map<String, Object> configProperties) {
    final ClientConfig clientConfig = new DefaultClientConfig();
    if (configProperties != null) {
      clientConfig.getProperties().putAll(configProperties);
    }

    client = Client.create(clientConfig);
    client.setFollowRedirects(false);

    webHDFSResource = client.resource(baseUrl).path("webhdfs/v1");
  }

  public void mkdir(String path) {
    webHDFSResource.path(path).queryParam("op", "MKDIRS").put();
  }

  public void sendFile(String path, boolean overwrite, String content) {
    final ClientResponse expectContinueResponse = webHDFSResource.path(path)
      .queryParam("op", "CREATE")
      .queryParam("overwrite", String.valueOf(overwrite))
      .header("Expect", "100-Continue")
      .put(ClientResponse.class);

    if (expectContinueResponse.getStatus() != 307) {
      throw new IllegalStateException(String.format("Unexpected response from hdfs namenode. Expected: 307, got: %d",
                                                    expectContinueResponse.getStatus()));
    }

    final URI redirectedLocation = expectContinueResponse.getLocation();
    final ClientResponse putResponse = client.resource(redirectedLocation).put(ClientResponse.class, content);

    if (putResponse.getStatus() != 201) {
      throw new IllegalStateException(String.format("Unexpected response from hdfs datanode with url %s. Expected: 201, got: %d",
                                                    redirectedLocation.toString(), putResponse.getStatus()));
    }
  }
}
