package ru.hh.hadoop.webhdfs;

import com.sun.jersey.api.client.ClientResponse;
import org.junit.Test;
import ru.hh.jersey.test.ActualRequest;
import ru.hh.jersey.test.ExpectedResponse;
import ru.hh.jersey.test.HttpMethod;
import ru.hh.jersey.test.JerseyClientTest;
import ru.hh.jersey.test.RequestMapping;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WebHDFSClientTest extends JerseyClientTest {
  @Test
  public void testMkDir() throws Exception {
    final String dirPath = "/test/dir/path";

    final WebHDFSClient webHDFSClient = new WebHDFSClient(getBaseURI().toString());

    final RequestMapping requestMapping = RequestMapping.builder(HttpMethod.PUT, "/webhdfs/v1" + dirPath)
      .addQueryParam("op", "MKDIRS")
      .build();

    final ExpectedResponse response = ExpectedResponse.builder().content("{\"boolean\": true}").mediaType("application/json").build();
    setServerAnswer(requestMapping, response);

    webHDFSClient.mkdir(dirPath);
  }

  @Test
  public void testSendFile() throws Exception {
    final String filePath = "/testFile";
    final WebHDFSClient webHDFSClient = new WebHDFSClient(getBaseURI().toString());

    final RequestMapping expectContinueRequest = RequestMapping.builder(HttpMethod.PUT, "/webhdfs/v1" + filePath)
      .addQueryParam("op", "CREATE")
      .addQueryParam("overwrite", "true")
      .build();
    final ExpectedResponse expectContinueResponse = ExpectedResponse.builder()
      .status(ClientResponse.Status.TEMPORARY_REDIRECT)
      .addHeader("Location", getBaseURI().toString() + "sendDataPath")
      .build();
    setServerAnswer(expectContinueRequest, expectContinueResponse);

    final RequestMapping sendDataRequest = RequestMapping.builder(HttpMethod.PUT, "/sendDataPath").build();
    final ExpectedResponse sendDataResponse = ExpectedResponse.builder()
      .status(ClientResponse.Status.CREATED)
      .build();
    setServerAnswer(sendDataRequest, sendDataResponse);

    webHDFSClient.sendFile(filePath, true, "test content");

    final List<ActualRequest> expectContinueActualRequestList = getActualRequests(expectContinueRequest);
    assertEquals(1, expectContinueActualRequestList.size());

    final ActualRequest expectContinueActualRequest = expectContinueActualRequestList.get(0);
    assertNull(expectContinueActualRequest.getContent());
    assertEquals("100-Continue", expectContinueActualRequest.getHeaders().getFirst("expect"));

    final List<ActualRequest> sendDataActualRequests = getActualRequests(sendDataRequest);
    assertEquals(1, sendDataActualRequests.size());

    final ActualRequest sendDataActualRequest = sendDataActualRequests.get(0);
    assertEquals("test content", sendDataActualRequest.getContent());
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExceptionWhenNoRedirectStatusInAnswerToExpectContinueSendFile() throws Exception {
    final String filePath = "/testFile";
    final WebHDFSClient webHDFSClient = new WebHDFSClient(getBaseURI().toString());

    final RequestMapping expectContinueRequest = RequestMapping.builder(HttpMethod.PUT, "/webhdfs/v1" + filePath)
      .addQueryParam("op", "CREATE")
      .addQueryParam("overwrite", "true")
      .build();
    final ExpectedResponse expectContinueResponse = ExpectedResponse.builder()
      .status(ClientResponse.Status.OK)
      .build();
    setServerAnswer(expectContinueRequest, expectContinueResponse);

    webHDFSClient.sendFile(filePath, true, "test content");
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExceptionWhenNoCreatedStatusInAnswerToSendFile() throws Exception {
    final String filePath = "/testFile";
    final WebHDFSClient webHDFSClient = new WebHDFSClient(getBaseURI().toString());

    final RequestMapping expectContinueRequest = RequestMapping.builder(HttpMethod.PUT, "/webhdfs/v1" + filePath)
      .addQueryParam("op", "CREATE")
      .addQueryParam("overwrite", "true")
      .build();
    final ExpectedResponse expectContinueResponse = ExpectedResponse.builder()
      .status(ClientResponse.Status.TEMPORARY_REDIRECT)
      .addHeader("Location", getBaseURI().toString() + "sendDataPath")
      .build();
    setServerAnswer(expectContinueRequest, expectContinueResponse);

    final RequestMapping sendDataRequest = RequestMapping.builder(HttpMethod.PUT, "/sendDataPath").build();
    final ExpectedResponse sendDataResponse = ExpectedResponse.builder()
      .status(ClientResponse.Status.OK)
      .build();
    setServerAnswer(sendDataRequest, sendDataResponse);

    webHDFSClient.sendFile(filePath, true, "test content");
  }
}
