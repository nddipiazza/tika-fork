package org.apache.tika.fork.main;

import org.apache.tika.client.TikaRunner;
import org.apache.tika.metadata.Metadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class TikaForkMainTest {

  private static final Logger LOG = LoggerFactory.getLogger(TikaForkMainTest.class);

  String xlsPath = "test-files" + File.separator + "xls-sample.xls";
  String txtPath = "test-files" + File.separator + "out.txt";
  String bombFilePath = "test-files" + File.separator + "bomb.xls";
  String zipBombPath = "test-files" + File.separator + "zip-bomb.zip";

  public static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  String [] args;

  int contentInServerPort;
  int metadataOutServerPort;
  int contentOutServerPort;

  @Before
  public void setArgs() throws Exception {
    contentInServerPort = findRandomOpenPortOnAllLocalInterfaces();
    metadataOutServerPort = findRandomOpenPortOnAllLocalInterfaces();
    contentOutServerPort = findRandomOpenPortOnAllLocalInterfaces();
    args = new String[] {
      "-workDirectoryPath",
        System.getProperty("java.io.tmpdir"),
        "-parserPropertiesFilePath",
        Paths.get("test-files", "parse.properties").toAbsolutePath().toString(),
        "-contentInServerPort",
        String.valueOf(contentInServerPort),
        "-metadataOutServerPort",
      String.valueOf(metadataOutServerPort),
        "-contentOutServerPort",
      String.valueOf(contentOutServerPort),
    };
  }
  
  @Test
  public void testMaxBytesExcel() throws Exception {

    ExecutorService singleThreadEx = Executors.newSingleThreadExecutor();

    singleThreadEx.execute(() -> {
      try {
        TikaForkMain.main(args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    TikaRunner tikaRunner = new TikaRunner(contentInServerPort, metadataOutServerPort, contentOutServerPort, true);

    ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
    try (FileInputStream fis = new FileInputStream(xlsPath)) {
      Metadata metadata = tikaRunner.parse(xlsPath,
        "application/vnd.ms-excel",
        fis,
        contentOutputStream,
        300000L,
        100
      );

      Assert.assertEquals(23, metadata.size());
      Assert.assertEquals(100, contentOutputStream.size());
    }

    singleThreadEx.shutdownNow();
  }

  @Test
  public void testFullExcel() throws Exception {
    
    ExecutorService singleThreadEx = Executors.newSingleThreadExecutor();

    singleThreadEx.execute(() -> {
      try {
        TikaForkMain.main(args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    TikaRunner tikaRunner = new TikaRunner(contentInServerPort, metadataOutServerPort, contentOutServerPort, true);

    ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
    try (FileInputStream fis = new FileInputStream(xlsPath)) {
      Metadata metadata = tikaRunner.parse(xlsPath,
        "application/vnd.ms-excel",
        fis,
        contentOutputStream,
        300000L,
        100000000
      );

      LOG.info("Metadata {}", metadata);

      Assert.assertEquals(metadata.size(), 23);
      Assert.assertEquals(contentOutputStream.size(), 4824);
    }

    singleThreadEx.shutdownNow();
  }

  @Test(expected= TimeoutException.class)
  public void testMaxBytesExcelBomb() throws Exception {
    ExecutorService singleThreadEx = Executors.newSingleThreadExecutor();

    singleThreadEx.execute(() -> {
      try {
        TikaForkMain.main(args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    TikaRunner tikaRunner = new TikaRunner(contentInServerPort, metadataOutServerPort, contentOutServerPort, true);

    ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
    try (FileInputStream fis = new FileInputStream(bombFilePath)) {
      Metadata metadata = tikaRunner.parse(bombFilePath,
        "application/vnd.ms-excel",
        fis,
        contentOutputStream,
        3000L,
        8000000
      );

      LOG.info("Metadata {}", metadata);

      System.out.println(new String(contentOutputStream.toByteArray()));
    }

    singleThreadEx.shutdownNow();
  }

  @Test
  public void testMaxBytesText() throws Exception {

    ExecutorService singleThreadEx = Executors.newSingleThreadExecutor();

    singleThreadEx.execute(() -> {
      try {
        TikaForkMain.main(args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    TikaRunner tikaRunner = new TikaRunner(contentInServerPort, metadataOutServerPort, contentOutServerPort, true);

    ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
    try (FileInputStream fis = new FileInputStream(txtPath)) {
      Metadata metadata = tikaRunner.parse(txtPath,
        "text/plain",
        fis,
        contentOutputStream,
        300000L,
        5000
      );

      LOG.info("Metadata {}", metadata);

      Assert.assertEquals(5000, contentOutputStream.size());

      Assert.assertEquals("test 0\n" +
        "test 1\n" +
        "test 2\n" +
        "test 3\n" +
        "test 4\n" +
        "test 5\n" +
        "test 6\n" +
        "test 7\n" +
        "test 8\n" +
        "test 9\n" +
        "test 10\n" +
        "test 11\n" +
        "test 12\n" +
        "test 13\n" +
        "test 14\n" +
        "test 15\n" +
        "test 16\n" +
        "test 17\n" +
        "test 18\n" +
        "test 19\n" +
        "test 20\n" +
        "test 21\n" +
        "test 22\n" +
        "test 23\n" +
        "test 24\n" +
        "test 25\n" +
        "test 26\n" +
        "test 27\n" +
        "test 28\n" +
        "test 29\n" +
        "test 30\n" +
        "test 31\n" +
        "test 32\n" +
        "test 33\n" +
        "test 34\n" +
        "test 35\n" +
        "test 36\n" +
        "test 37\n" +
        "test 38\n" +
        "test 39\n" +
        "test 40\n" +
        "test 41\n" +
        "test 42\n" +
        "test 43\n" +
        "test 44\n" +
        "test 45\n" +
        "test 46\n" +
        "test 47\n" +
        "test 48\n" +
        "test 49\n" +
        "test 50\n" +
        "test 51\n" +
        "test 52\n" +
        "test 53\n" +
        "test 54\n" +
        "test 55\n" +
        "test 56\n" +
        "test 57\n" +
        "test 58\n" +
        "test 59\n" +
        "test 60\n" +
        "test 61\n" +
        "test 62\n" +
        "test 63\n" +
        "test 64\n" +
        "test 65\n" +
        "test 66\n" +
        "test 67\n" +
        "test 68\n" +
        "test 69\n" +
        "test 70\n" +
        "test 71\n" +
        "test 72\n" +
        "test 73\n" +
        "test 74\n" +
        "test 75\n" +
        "test 76\n" +
        "test 77\n" +
        "test 78\n" +
        "test 79\n" +
        "test 80\n" +
        "test 81\n" +
        "test 82\n" +
        "test 83\n" +
        "test 84\n" +
        "test 85\n" +
        "test 86\n" +
        "test 87\n" +
        "test 88\n" +
        "test 89\n" +
        "test 90\n" +
        "test 91\n" +
        "test 92\n" +
        "test 93\n" +
        "test 94\n" +
        "test 95\n" +
        "test 96\n" +
        "test 97\n" +
        "test 98\n" +
        "test 99\n" +
        "test 100\n" +
        "test 101\n" +
        "test 102\n" +
        "test 103\n" +
        "test 104\n" +
        "test 105\n" +
        "test 106\n" +
        "test 107\n" +
        "test 108\n" +
        "test 109\n" +
        "test 110\n" +
        "test 111\n" +
        "test 112\n" +
        "test 113\n" +
        "test 114\n" +
        "test 115\n" +
        "test 116\n" +
        "test 117\n" +
        "test 118\n" +
        "test 119\n" +
        "test 120\n" +
        "test 121\n" +
        "test 122\n" +
        "test 123\n" +
        "test 124\n" +
        "test 125\n" +
        "test 126\n" +
        "test 127\n" +
        "test 128\n" +
        "test 129\n" +
        "test 130\n" +
        "test 131\n" +
        "test 132\n" +
        "test 133\n" +
        "test 134\n" +
        "test 135\n" +
        "test 136\n" +
        "test 137\n" +
        "test 138\n" +
        "test 139\n" +
        "test 140\n" +
        "test 141\n" +
        "test 142\n" +
        "test 143\n" +
        "test 144\n" +
        "test 145\n" +
        "test 146\n" +
        "test 147\n" +
        "test 148\n" +
        "test 149\n" +
        "test 150\n" +
        "test 151\n" +
        "test 152\n" +
        "test 153\n" +
        "test 154\n" +
        "test 155\n" +
        "test 156\n" +
        "test 157\n" +
        "test 158\n" +
        "test 159\n" +
        "test 160\n" +
        "test 161\n" +
        "test 162\n" +
        "test 163\n" +
        "test 164\n" +
        "test 165\n" +
        "test 166\n" +
        "test 167\n" +
        "test 168\n" +
        "test 169\n" +
        "test 170\n" +
        "test 171\n" +
        "test 172\n" +
        "test 173\n" +
        "test 174\n" +
        "test 175\n" +
        "test 176\n" +
        "test 177\n" +
        "test 178\n" +
        "test 179\n" +
        "test 180\n" +
        "test 181\n" +
        "test 182\n" +
        "test 183\n" +
        "test 184\n" +
        "test 185\n" +
        "test 186\n" +
        "test 187\n" +
        "test 188\n" +
        "test 189\n" +
        "test 190\n" +
        "test 191\n" +
        "test 192\n" +
        "test 193\n" +
        "test 194\n" +
        "test 195\n" +
        "test 196\n" +
        "test 197\n" +
        "test 198\n" +
        "test 199\n" +
        "test 200\n" +
        "test 201\n" +
        "test 202\n" +
        "test 203\n" +
        "test 204\n" +
        "test 205\n" +
        "test 206\n" +
        "test 207\n" +
        "test 208\n" +
        "test 209\n" +
        "test 210\n" +
        "test 211\n" +
        "test 212\n" +
        "test 213\n" +
        "test 214\n" +
        "test 215\n" +
        "test 216\n" +
        "test 217\n" +
        "test 218\n" +
        "test 219\n" +
        "test 220\n" +
        "test 221\n" +
        "test 222\n" +
        "test 223\n" +
        "test 224\n" +
        "test 225\n" +
        "test 226\n" +
        "test 227\n" +
        "test 228\n" +
        "test 229\n" +
        "test 230\n" +
        "test 231\n" +
        "test 232\n" +
        "test 233\n" +
        "test 234\n" +
        "test 235\n" +
        "test 236\n" +
        "test 237\n" +
        "test 238\n" +
        "test 239\n" +
        "test 240\n" +
        "test 241\n" +
        "test 242\n" +
        "test 243\n" +
        "test 244\n" +
        "test 245\n" +
        "test 246\n" +
        "test 247\n" +
        "test 248\n" +
        "test 249\n" +
        "test 250\n" +
        "test 251\n" +
        "test 252\n" +
        "test 253\n" +
        "test 254\n" +
        "test 255\n" +
        "test 256\n" +
        "test 257\n" +
        "test 258\n" +
        "test 259\n" +
        "test 260\n" +
        "test 261\n" +
        "test 262\n" +
        "test 263\n" +
        "test 264\n" +
        "test 265\n" +
        "test 266\n" +
        "test 267\n" +
        "test 268\n" +
        "test 269\n" +
        "test 270\n" +
        "test 271\n" +
        "test 272\n" +
        "test 273\n" +
        "test 274\n" +
        "test 275\n" +
        "test 276\n" +
        "test 277\n" +
        "test 278\n" +
        "test 279\n" +
        "test 280\n" +
        "test 281\n" +
        "test 282\n" +
        "test 283\n" +
        "test 284\n" +
        "test 285\n" +
        "test 286\n" +
        "test 287\n" +
        "test 288\n" +
        "test 289\n" +
        "test 290\n" +
        "test 291\n" +
        "test 292\n" +
        "test 293\n" +
        "test 294\n" +
        "test 295\n" +
        "test 296\n" +
        "test 297\n" +
        "test 298\n" +
        "test 299\n" +
        "test 300\n" +
        "test 301\n" +
        "test 302\n" +
        "test 303\n" +
        "test 304\n" +
        "test 305\n" +
        "test 306\n" +
        "test 307\n" +
        "test 308\n" +
        "test 309\n" +
        "test 310\n" +
        "test 311\n" +
        "test 312\n" +
        "test 313\n" +
        "test 314\n" +
        "test 315\n" +
        "test 316\n" +
        "test 317\n" +
        "test 318\n" +
        "test 319\n" +
        "test 320\n" +
        "test 321\n" +
        "test 322\n" +
        "test 323\n" +
        "test 324\n" +
        "test 325\n" +
        "test 326\n" +
        "test 327\n" +
        "test 328\n" +
        "test 329\n" +
        "test 330\n" +
        "test 331\n" +
        "test 332\n" +
        "test 333\n" +
        "test 334\n" +
        "test 335\n" +
        "test 336\n" +
        "test 337\n" +
        "test 338\n" +
        "test 339\n" +
        "test 340\n" +
        "test 341\n" +
        "test 342\n" +
        "test 343\n" +
        "test 344\n" +
        "test 345\n" +
        "test 346\n" +
        "test 347\n" +
        "test 348\n" +
        "test 349\n" +
        "test 350\n" +
        "test 351\n" +
        "test 352\n" +
        "test 353\n" +
        "test 354\n" +
        "test 355\n" +
        "test 356\n" +
        "test 357\n" +
        "test 358\n" +
        "test 359\n" +
        "test 360\n" +
        "test 361\n" +
        "test 362\n" +
        "test 363\n" +
        "test 364\n" +
        "test 365\n" +
        "test 366\n" +
        "test 367\n" +
        "test 368\n" +
        "test 369\n" +
        "test 370\n" +
        "test 371\n" +
        "test 372\n" +
        "test 373\n" +
        "test 374\n" +
        "test 375\n" +
        "test 376\n" +
        "test 377\n" +
        "test 378\n" +
        "test 379\n" +
        "test 380\n" +
        "test 381\n" +
        "test 382\n" +
        "test 383\n" +
        "test 384\n" +
        "test 385\n" +
        "test 386\n" +
        "test 387\n" +
        "test 388\n" +
        "test 389\n" +
        "test 390\n" +
        "test 391\n" +
        "test 392\n" +
        "test 393\n" +
        "test 394\n" +
        "test 395\n" +
        "test 396\n" +
        "test 397\n" +
        "test 398\n" +
        "test 399\n" +
        "test 400\n" +
        "test 401\n" +
        "test 402\n" +
        "test 403\n" +
        "test 404\n" +
        "test 405\n" +
        "test 406\n" +
        "test 407\n" +
        "test 408\n" +
        "test 409\n" +
        "test 410\n" +
        "test 411\n" +
        "test 412\n" +
        "test 413\n" +
        "test 414\n" +
        "test 415\n" +
        "test 416\n" +
        "test 417\n" +
        "test 418\n" +
        "test 419\n" +
        "test 420\n" +
        "test 421\n" +
        "test 422\n" +
        "test 423\n" +
        "test 424\n" +
        "test 425\n" +
        "test 426\n" +
        "test 427\n" +
        "test 428\n" +
        "test 429\n" +
        "test 430\n" +
        "test 431\n" +
        "test 432\n" +
        "test 433\n" +
        "test 434\n" +
        "test 435\n" +
        "test 436\n" +
        "test 437\n" +
        "test 438\n" +
        "test 439\n" +
        "test 440\n" +
        "test 441\n" +
        "test 442\n" +
        "test 443\n" +
        "test 444\n" +
        "test 445\n" +
        "test 446\n" +
        "test 447\n" +
        "test 448\n" +
        "test 449\n" +
        "test 450\n" +
        "test 451\n" +
        "test 452\n" +
        "test 453\n" +
        "test 454\n" +
        "test 455\n" +
        "test 456\n" +
        "test 457\n" +
        "test 458\n" +
        "test 459\n" +
        "test 460\n" +
        "test 461\n" +
        "test 462\n" +
        "test 463\n" +
        "test 464\n" +
        "test 465\n" +
        "test 466\n" +
        "test 467\n" +
        "test 468\n" +
        "test 469\n" +
        "test 470\n" +
        "test 471\n" +
        "test 472\n" +
        "test 473\n" +
        "test 474\n" +
        "test 475\n" +
        "test 476\n" +
        "test 477\n" +
        "test 478\n" +
        "test 479\n" +
        "test 480\n" +
        "test 481\n" +
        "test 482\n" +
        "test 483\n" +
        "test 484\n" +
        "test 485\n" +
        "test 486\n" +
        "test 487\n" +
        "test 488\n" +
        "test 489\n" +
        "test 490\n" +
        "test 491\n" +
        "test 492\n" +
        "test 493\n" +
        "test 494\n" +
        "test 495\n" +
        "test 496\n" +
        "test 497\n" +
        "test 498\n" +
        "test 499\n" +
        "test 500\n" +
        "test 501\n" +
        "test 502\n" +
        "test 503\n" +
        "test 504\n" +
        "test 505\n" +
        "test 506\n" +
        "test 507\n" +
        "test 508\n" +
        "test 509\n" +
        "test 510\n" +
        "test 511\n" +
        "test 512\n" +
        "test 513\n" +
        "test 514\n" +
        "test 515\n" +
        "test 516\n" +
        "test 517\n" +
        "test 518\n" +
        "test 519\n" +
        "test 520\n" +
        "test 521\n" +
        "test 522\n" +
        "test 523\n" +
        "test 524\n" +
        "test 525\n" +
        "test 526\n" +
        "test 527\n" +
        "test 528\n" +
        "test 529\n" +
        "test 530\n" +
        "test 531\n" +
        "test 532\n" +
        "test 533\n" +
        "test 534\n" +
        "test 535\n" +
        "test 536\n" +
        "test 537\n" +
        "test 538\n" +
        "test 539\n" +
        "test 540\n" +
        "test 541\n" +
        "test 542\n" +
        "test 543\n" +
        "test 544\n" +
        "test 545\n" +
        "test 546\n" +
        "test 547\n" +
        "test 548\n" +
        "test 549\n" +
        "test 550\n" +
        "test 551\n" +
        "test 552\n" +
        "test 553\n" +
        "test 554\n" +
        "test 555\n" +
        "test 556\n" +
        "test 557\n" +
        "test 558\n" +
        "test 559\n" +
        "test 560\n" +
        "test 561\n" +
        "test 562\n" +
        "test 563\n" +
        "test 564\n" +
        "test 565\n" +
        "test 566\n" +
        "test 56", new String(contentOutputStream.toByteArray()));
    }
  }

  @Test
  public void testMaxBytesZipBomb() throws Exception {
    ExecutorService singleThreadEx = Executors.newSingleThreadExecutor();

    singleThreadEx.execute(() -> {
      try {
        TikaForkMain.main(args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    TikaRunner tikaRunner = new TikaRunner(contentInServerPort, metadataOutServerPort, contentOutServerPort, true);

    ByteArrayOutputStream contentOutputStream = new ByteArrayOutputStream();
    try (FileInputStream fis = new FileInputStream(zipBombPath)) {
      Metadata metadata = null;
      try {
        metadata = tikaRunner.parse(zipBombPath,
          "application/zip",
          fis,
          contentOutputStream,
          4000L,
          10000
        );
        Assert.fail("expected timeout");
      } catch (TimeoutException e) {
        LOG.info("Got expected timeout");
      }
      Assert.assertEquals(10000, contentOutputStream.size());
    }

    singleThreadEx.shutdownNow();
  }
}
