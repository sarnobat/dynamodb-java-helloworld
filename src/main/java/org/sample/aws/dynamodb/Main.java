package org.sample.aws.dynamodb;

import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Consumer;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;

public class Main {

  public static void main(String[] args) {

    // Create
    {
      for (int i = 0; i < 10; i++) {
        PutItemOutcome outcome =
            new DynamoDB(
                    AmazonDynamoDBClientBuilder.standard()
                        .withRegion(Regions.US_EAST_1.name())
                        .build())
                .getTable("apple_siri_free_keys_devint")
                .putItem(
                    new Item()
                        .withPrimaryKey("key_base64", "123" + System.currentTimeMillis())
                        .withNumber("Price", 500)
                        .withStringSet("Color", new HashSet<String>(Arrays.asList("Red", "Black")))
                        .withString("ProductCategory", "Bicycle")
                        .withBoolean("InStock", true));
      }
    }

    // Read
    {
      new DynamoDB(
              AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1.name()).build())
          .getTable("apple_siri_free_keys_devint")
          .scan()
          .forEach(
              new Consumer<Item>() {
                public void accept(Item t) {
                  System.out.println(t.toJSONPretty());
                }
              });
    }

    // Count
    {
      int count =
          new DynamoDB(
                  AmazonDynamoDBClientBuilder.standard()
                      .withRegion(Regions.US_EAST_1.name())
                      .build())
              .getTable("apple_siri_free_keys_devint")
              .scan()
              .firstPage()
              .getLowLevelResult()
              .getItems()
              .size();
      System.out.println("SRIDHAR App.main() - count after insertion = " + count);
    }

    // Delete
    {
      for (Item i :
          new DynamoDB(
                  AmazonDynamoDBClientBuilder.standard()
                      .withRegion(Regions.US_EAST_1.name())
                      .build())
              .getTable("apple_siri_free_keys_devint")
              .scan()
              .firstPage()
              .getLowLevelResult()
              .getItems()) {

        new DynamoDB(
                AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1.name()).build())
            .getTable("apple_siri_free_keys_devint")
            .deleteItem("key_base64", i.asMap().get("key_base64").toString());

        int count =
            new DynamoDB(
                    AmazonDynamoDBClientBuilder.standard()
                        .withRegion(Regions.US_EAST_1.name())
                        .build())
                .getTable("apple_siri_free_keys_devint")
                .scan()
                .firstPage()
                .getLowLevelResult()
                .getItems()
                .size();
        System.out.println("SRIDHAR App.main() - count after deletion = " + count);
      }
    }
  }
}
